# Changelog

## 0.0.11
  * Updates `requests` to version `2.20.0` in response to CVE 2018-18074 [#13](https://github.com/singer-io/tap-appsflyer/pull/13)

## 0.0.10
  * Remove `"format": "uri"` from in_app_events schema [#12](https://github.com/singer-io/tap-appsflyer/pull/12)

## 0.0.9
  * Fixes infinite loop on `in_app_events` stream [#11](https://github.com/singer-io/tap-appsflyer/pull/11)

## 0.0.8
  * Allow booleans to be `None` in traqnsform code [#9](https://github.com/singer-io/tap-appsflyer/pull/9)

## 0.0.7
  * Fix syntax error in `installations.json` schema from 0.0.6

## 0.0.6
  * Remove the `"format": "uri"` elements from installations json schema

## 0.0.5
  * Syncing in-app-events would take too long so cut the window down and iterate forward [#7](https://github.com/singer-io/tap-appsflyer/pull/7)

## 0.0.3
  * Updated the schema for installations to set 'customer_user_id' to allow a string value [#5](https://github.com/singer-io/tap-appsflyer/pull/5)

## 0.0.2
  * Updated the schema for in_app_events to set `customer_user_id` to allow a string value [#4](https://github.com/singer-io/tap-appsflyer/pull/4)
