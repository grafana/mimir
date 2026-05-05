local parseDuration(d) =
  if std.endsWith(d, 'ms') then std.parseJson(std.substr(d, 0, std.length(d) - 2)) / 1000
  else if std.endsWith(d, 's') then std.parseJson(std.substr(d, 0, std.length(d) - 1))
  else if std.endsWith(d, 'm') then std.parseJson(std.substr(d, 0, std.length(d) - 1)) * 60
  else if std.endsWith(d, 'h') then std.parseJson(std.substr(d, 0, std.length(d) - 1)) * 3600
  else error 'unable to parse duration: %s' % d;

local formatDuration(seconds) =
  if seconds % 3600 == 0 then '%dh' % std.floor(seconds / 3600)
  else if seconds % 60 == 0 then '%dm' % std.floor(seconds / 60)
  else '%ds' % seconds;

{
  // Returns a safe window for rate() and similar functions, ensuring it is large enough
  // relative to the configured scrape interval.
  // See https://www.robustperception.io/what-range-should-i-use-with-rate/
  rateInterval(min_duration)::
    formatDuration(std.max(parseDuration(min_duration), 4 * parseDuration($._config.scrape_interval))),

  // Returns a safe step for subquery notation (e.g. [range:step]), matching the scrape interval
  // so we don't query at a finer resolution than the data is collected.
  stepInterval(min_interval)::
    formatDuration(std.max(parseDuration(min_interval), parseDuration($._config.scrape_interval))),
}
