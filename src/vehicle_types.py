"""Best-effort vehicle/unit classification and vehicle number extraction.

Maps the small set of vehicle-type abbreviations already in
abbreviation_import.py to a broad category, used to pick a fallback icon and
as a lookup key for user-uploaded vehicle photos (see database.py's
vehicle_photos table). This is deliberately a coarse classification, not a
decode of every vehicle. Per vehicle type there's a bundled, freely licensed
stock photo (see STOCK_IMAGES), picked from the fire brigade number's type
digit where there is one; user-uploaded photos keyed by the exact vehicle
number or the category take precedence.
"""

import os
import re

# Abbreviation -> broad vehicle category. Only codes that identify a
# *vehicle/unit type* belong here (not e.g. priority codes or region codes).
VEHICLE_CATEGORIES: dict[str, str] = {
    # Ambulance
    "AMBU": "ambulance",
    "MUG": "ambulance",
    # Helicopters (air ambulance / trauma team)
    "MUH": "helikopter",
    "MAA": "helikopter",
    "HTT": "helikopter",
    # Fire - pumping/engine appliances
    "TS": "brandweer_tankautospuit",
    "TAS": "brandweer_tankautospuit",
    "TS-HV": "brandweer_tankautospuit",
    # Fire - aerial appliances
    "AL": "brandweer_ladder",
    "AL-42": "brandweer_ladder",
    "AL-36": "brandweer_ladder",
    "AL-32": "brandweer_ladder",
    "HW": "brandweer_hoogwerker",
    "HW-36": "brandweer_hoogwerker",
    "HW-24": "brandweer_hoogwerker",
    # Fire - specialized appliances
    "SL": "brandweer_slangenwagen",
    "SL-4": "brandweer_slangenwagen",
    "SL-6": "brandweer_slangenwagen",
    "WO": "brandweer_waterongevallen",
    "PB": "brandweer_poederblus",
    "SB": "brandweer_schuimblus",
    "MS": "brandweer_motorspuit",
    "DPA": "brandweer_dompelpomp",
    "DPU": "brandweer_dompelpomp",
    "RIV": "brandweer_riv",
    "HVH": "brandweer_haakarmbak",
    "GSH": "brandweer_haakarmbak",
    "VZH": "brandweer_haakarmbak",
    "LHH": "brandweer_haakarmbak",
    "CHH": "brandweer_haakarmbak",
    # Police
    "POL": "politie",
    "AE": "politie",
    "AT": "politie",
    "ME": "politie",
    # Water rescue
    "RB": "reddingboot",
}

# Category -> fallback emoji shown when no user-uploaded photo exists for
# either the exact vehicle number or the category itself.
CATEGORY_ICONS: dict[str, str] = {
    "ambulance": "🚑",
    "helikopter": "🚁",
    "brandweer_tankautospuit": "🚒",
    "brandweer_ladder": "🚒",
    "brandweer_hoogwerker": "🚒",
    "brandweer_slangenwagen": "🚒",
    "brandweer_waterongevallen": "🚤",
    "brandweer_poederblus": "🚒",
    "brandweer_schuimblus": "🚒",
    "brandweer_motorspuit": "🚒",
    "brandweer_dompelpomp": "🚒",
    "brandweer_riv": "🚒",
    "brandweer_haakarmbak": "🚛",
    "politie": "🚓",
    "reddingboot": "🛟",
}

DEFAULT_ICON = "📻"

# Matches a known vehicle-type token directly followed by a vehicle number,
# either the official "RR-XXXX" fire brigade format (e.g. "TS 07-1782") or a
# bare run of digits as commonly seen for ambulance units (e.g. "AMBU 17106").
# Longer codes (AL-42, ...) are tried first so e.g. "AL-42" isn't matched as
# bare "AL", and the dashed number form is tried before the bare-digits one.
_VEHICLE_NUMBER_RE = re.compile(
    r"\b(?:"
    + "|".join(re.escape(k) for k in sorted(VEHICLE_CATEGORIES, key=len, reverse=True))
    + r")\s+(\d{2,4}-\d{3,4}|\d{3,6})\b"
)

# Fallback for ambulance "ritmelding" messages that carry the vehicle number
# without any AMBU/MUG-style prefix right next to it, as a bare 5-digit
# RRNNN token - 2-digit RAV region + 3-digit unit sequence, dash omitted to
# save space in the pager text, e.g. "07205 - Arnhem Rit 264112" is unit
# 07-205 (Gelderland-Midden, which includes Arnhem). Confirmed against
# AmbuMedia's public vehicle registry (ambumedia.nl/07-gelderland-midden),
# which lists 07-205 as an active Mercedes Sprinter mid-complexity-care
# unit. The trailing "Rit <ritnummer>" anchors the match so this doesn't
# misfire on unrelated numbers elsewhere in the body. Reassembled with a
# dash on the way out to match the official RR-NNN format.
_AMBULANCE_UNIT_RE = re.compile(r"\b(\d{2})(\d{3})\s*-\s*[A-Za-zÀ-ÿ'\-]+(?:\s+[A-Za-zÀ-ÿ'\-]+)*\s+Rit\s+\d+\b")


def classify(abbreviations: list[dict[str, str]]) -> str:
    """Pick the first recognized vehicle category from a message's found abbreviations."""
    for item in abbreviations:
        category = VEHICLE_CATEGORIES.get(item.get("abbreviation", ""))
        if category:
            return category
    return ""


def extract_vehicle_number(text: str) -> str:
    """Extract a vehicle number following a known vehicle-type token, if present."""
    match = _VEHICLE_NUMBER_RE.search(text)
    if match:
        number = match.group(1)
        # Bare 5-digit ambulance numbers (e.g. "AMBU 18187") are RRNNN - the
        # same 2-digit region + 3-digit sequence scheme confirmed for the
        # "<RRNNN> - <plaats> Rit <ritnummer>" format below, just written
        # without a dash here. Reformat for consistency. Numbers that already
        # contain a dash (fire brigade "RR-XXXX") or aren't 5 digits are left
        # as-is - unverified for those cases.
        if "-" not in number and len(number) == 5:
            number = f"{number[:2]}-{number[2:]}"
        return number
    match = _AMBULANCE_UNIT_RE.search(text)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return ""


def icon_for(category: str) -> str:
    """Fallback emoji for a vehicle category ("" gives a generic radio icon)."""
    return CATEGORY_ICONS.get(category, DEFAULT_ICON)


# Bundled stock photos (vehicle_images/<name>.webp): freely licensed photos
# from Wikimedia Commons, background removed and licence plates replaced.
# Shown when the user hasn't uploaded a photo for the vehicle or category.
STOCK_IMAGES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vehicle_images")
STOCK_IMAGES: dict[str, dict[str, str]] = {
    "tankautospuit": {
        "author": "Alf van Beem",
        "license": "CC0",
        "source": "https://commons.wikimedia.org/wiki/File:Volvo_FE_Ziegler_Brandweer_Midden-_en_West_Brabant,_Gilze_en_Rijen.JPG",
    },
    "ladder": {
        "author": "Alf van Beem",
        "license": "CC0",
        "source": "https://commons.wikimedia.org/wiki/File:Ladderwagen_Brandweer_Velsen_Unit_6602.JPG",
    },
    "hulpverlening": {
        "author": "Alf van Beem",
        "license": "CC0",
        "source": "https://commons.wikimedia.org/wiki/File:Brandweer_Kennemerland_Hoofddorp_12-4270_Volvo_FM_Ziegler_foto1.jpg",
    },
    "haakarm": {
        "author": "Alf van Beem",
        "license": "Public domain",
        "source": "https://commons.wikimedia.org/wiki/File:Volvo_FM,_Unit_17-1381_%26_Unit_17-1362,_Gezamenlijke_Brandweer_Kazerne_Prinses_M%C3%A1ximaweg_pic1.JPG",
    },
    "schuimblus": {
        "author": "S.J. de Waard",
        "license": "CC BY-SA 4.0",
        "source": "https://commons.wikimedia.org/wiki/File:Schuimblusaanhanger_17-1865_van_Brandweer_Rotterdam-Rijnmond_(01).jpg",
    },
    "waterongevallen": {
        "author": "Alf van Beem",
        "license": "CC0",
        "source": "https://commons.wikimedia.org/wiki/File:Brandweer_Kennemerland_Hoofddorp_Mercedes_416_BS-GJ-48_car_354_Waterongevallen.JPG",
    },
    "officier": {
        "author": "Alf van Beem",
        "license": "Public domain",
        "source": "https://commons.wikimedia.org/wiki/File:Nissan_Brandweer_Kennemerland_Officier_van_Dienst_unit_12-1293_foto_2.JPG",
    },
    "ambulance": {
        "author": "Joost J. Bakker IJmuiden",
        "license": "CC BY 2.0",
        "source": "https://commons.wikimedia.org/wiki/File:Kennemerland_Mercedes_Benz_Sprinter_Ambulance_12-511.jpg",
    },
    "helikopter": {
        "author": "BoasB112",
        "license": "CC BY-SA 4.0",
        "source": "https://commons.wikimedia.org/wiki/File:Lifeliner_2_na_een_inzet.jpg",
    },
    "reddingboot": {
        "author": "kees torn",
        "license": "CC BY-SA 2.0",
        "source": "https://commons.wikimedia.org/wiki/File:Open_dag_KNRM_Hoek_van_Holland_30-4-2016._(26126938383).jpg",
    },
    "politie": {
        "author": "Joost J. Bakker",
        "license": "CC BY 2.0",
        "source": "https://commons.wikimedia.org/wiki/File:BMW_X1_Politie.jpg",
    },
}

# Vehicle category -> stock photo
CATEGORY_STOCK_IMAGES: dict[str, str] = {
    "ambulance": "ambulance",
    "helikopter": "helikopter",
    "brandweer_tankautospuit": "tankautospuit",
    "brandweer_ladder": "ladder",
    "brandweer_hoogwerker": "ladder",
    "brandweer_slangenwagen": "hulpverlening",
    "brandweer_waterongevallen": "waterongevallen",
    "brandweer_poederblus": "schuimblus",
    "brandweer_schuimblus": "schuimblus",
    "brandweer_motorspuit": "tankautospuit",
    "brandweer_dompelpomp": "haakarm",
    "brandweer_riv": "officier",
    "brandweer_haakarmbak": "haakarm",
    "politie": "politie",
    "reddingboot": "reddingboot",
}

# Fire brigade vehicle numbers follow the Nationaal Nummerplan Brandweer
# Nederland: RR-KKTV = region, station, type of equipment, sequence number.
# The type digit tells the kind of vehicle even when the abbreviation doesn't.
_FIRE_NUMBER_RE = re.compile(r"^\d{2}-\d{2}(\d)\d$")
FIRE_TYPE_STOCK_IMAGES: dict[str, str] = {
    "0": "officier",  # Personenvervoer (dienstauto, dienstbus)
    "1": "waterongevallen",  # Waterongevallen
    "2": "hulpverlening",  # Ongeval gevaarlijke stoffen
    "3": "tankautospuit",  # Tankautospuit
    "4": "tankautospuit",  # TS voor bos/duinbrandbestrijding
    "5": "ladder",  # Redmaterieel (autoladder, hoogwerker)
    "6": "schuimblus",  # Bijzonder brandbestrijdingsmaterieel
    "7": "hulpverlening",  # Hulpverleningsmaterieel
    "8": "haakarm",  # Overig materieel (haakarm, kraan)
    "9": "officier",  # Staf- en commandomaterieel (OvD, HOvD, CvD)
}


def stock_image_for(category: str, number: str) -> str:
    """Name of the bundled photo for a vehicle ("" if none fits).

    A fire brigade number's type digit is more specific than the category,
    which only reflects the abbreviation in the message.
    """
    match = _FIRE_NUMBER_RE.match(number or "")
    if match and (category or "").startswith("brandweer"):
        return FIRE_TYPE_STOCK_IMAGES[match.group(1)]
    return CATEGORY_STOCK_IMAGES.get(category, "")
