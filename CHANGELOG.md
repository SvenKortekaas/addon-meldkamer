# Changelog

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
