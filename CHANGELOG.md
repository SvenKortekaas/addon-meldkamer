# Changelog

## 1.2.4

- The map keeps your zoom level: focusing a new incident moves the map without zooming in or out, and the last map position and zoom are restored after a reload.

## 1.2.3

- The sensor filter uses exactly the same style as the view tabs.
- Tab and filter labels are centred exactly.
- The sound, speech, auto-focus and theme buttons form one group in the tab style; enabled ones are highlighted.
- On screens up to 1400 px wide the sensor filter is a dropdown instead of a row of buttons.
- The live feed no longer freezes when the connection is silently dropped (e.g. overnight): the add-on sends a heartbeat and the dashboard reconnects by itself when it stops hearing from it. A rejected reconnect (shown as a lasting "Reconnecting…") is retried after 5 seconds.
- Loading the message list retries before showing "Failed to load messages", which now includes the reason.
- Fits smaller screens (1280–1400 px wide, up to 800 px high): compact top bar, slimmer Console side column, shorter Wallboard ticker.

## 1.2.2

- The sensor filter in the top bar is a labelled, outlined control like the view tabs, so it stands out in every view.
- Bigger Wallboard ticker (taller, 15 px message text over up to three lines) and a bigger clock.
- Removed the unused `/config` and `/share` folder mappings (Supervisor warned about the deprecated `config` mapping); `/ssl` stays for MQTT TLS certificates.

## 1.2.1

- Tabs: **Settings** (was Database; now also holds the ppm calibration) and **Info** (was Advanced), in that order after the views.
- Message detail always shows the vehicle photo slot, the date and distance from home, an "escalated" badge on the incident, the raw message, and Previous/Next to step through messages.
- Wallboard and Lanes show the detail floating in the map's corner instead of a centred popup; dialogs no longer dim the page.
- Draggable dividers between the panes of Console, Wallboard and Lanes (double-click resets).
- Console's map and detail column grows with the screen; lanes are capped so the map gets the rest.
- Messages with an A/B urgency code that also reach fire brigade capcodes go in the Ambulance lane.
- Requires Home Assistant 2026.9 or newer (map tiles always come through its map tile proxy).
- Fix: the built-in abbreviation dictionary is seeded on startup again.
- Settings and Info pages use the dashboard's look: flat panels, compact buttons, mono values, no emoji headings, and larger, more readable text.
- New screenshots.

## 1.2.0

- New compact dashboard with three views, switched with tabs in the top bar:
  - **Console**: a dense message table, with the map and the full message detail (vehicle photo, incident timeline, earlier messages at the address) docked on the right.
  - **Wallboard**: a large map with live counts, incidents sorted by priority, and a ticker of the newest messages.
  - **Lanes**: one column each for Brandweer, Ambulance, Politie and other disciplines, next to the map.
- Wallboard and Lanes open the message detail in a popup; Console shows it docked.
- The theme follows Home Assistant's light/dark mode, with a button to force light or dark.
- Fits one screen: receiver status, messages per hour, frames per minute and open incidents in the top bar; decoder and device status in a status bar.

## 1.1.1

- Prebuilt images: the add-on is now pulled from `ghcr.io/cyberjunky/addon-meldkamer` instead of being compiled on your system, so installing and updating take a minute instead of a long build.
- Based on Alpine 3.23 (Home Assistant's multi-arch base image).
- Only 64-bit systems (`amd64`, `aarch64`) are supported, the architectures Home Assistant's image builder supports.

## 1.1.0

### Breaking changes

- Sensor location filters now use Home Assistant zones: `center_lat`, `center_lon` and `radius_km` are replaced by `zone` (e.g. `zone: zone.home`), which supplies both the centre and the radius. Create a zone in Home Assistant for any other area you filtered on.
- The `distance` message attribute is replaced by `distance_km` (distance from `zone.home`). Sensor attributes still carry `distance`, now the distance to the centre of that sensor's zone.
- Sensor states are the spoken (TTS) text of the message; `body` holds the original.

### New

- `meldkamer_message` event for every message and `meldkamer_incident_escalated` when an incident escalates (`ha_events`).
- Receiver health sensors: `binary_sensor.meldkamer_receiver_online` and `sensor.meldkamer_messages_per_hour` (REST and MQTT).
- Announce messages on Home Assistant speakers with `tts.speak` (`announce` options, `announce: true` per sensor).
- Messages to the same address are grouped into incidents on the dashboard (`incident_window_minutes`).
- Repeat suppression for messages the pager network resends (`repeat_window_seconds`).
- Map history heatmap with a time slider.
- Reception quality on the Advanced tab: FLEX frames per minute, corrected bit errors, failed decodes, lost syncs.
- Automatic ppm calibration on the Advanced tab, saved to the add-on configuration.
- Bias-tee option for RTL-SDR (`receiver.bias_tee`).
- Failover to a backup `rtl_tcp` receiver (`receiver.backup_host`), and an automatic restart when the receiver stops producing frames (`receiver.offline_after_minutes`).
- README example for Home Assistant's Map card.

### Changed

- Browser speech, sensors, MQTT and speakers all use the same server-side TTS text.
- Removed unused POCSAG parsing and environment-variable configuration.
