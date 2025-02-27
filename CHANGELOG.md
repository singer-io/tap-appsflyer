# Changelog

## 0.3.0
  * Complete refactoring of the tap [#47](https://github.com/singer-io/tap-appsflyer/pull/47)
  * Bump version of `requests` dependency to 2.32.3
  * `singer-python` upgrade to 6.1.0 and `backoff` to 2.2.1

## 0.2.0
  * singer-python version upgrade to 5.13.0
  * Code changes to make code compatible with the latest singer-python version[#17](https://github.com/singer-io/tap-appsflyer/pull/17)

## 0.1.0
  * Support for api token v2
  * Dependabot update [#43](https://github.com/singer-io/tap-appsflyer/pull/43)

## 0.0.13
  * Add organic installations[#20](https://github.com/singer-io/tap-appsflyer/pull/20)
  * Add Helper Function to remove whitespaces from config and respective unittest[#29](https://github.com/singer-io/tap-appsflyer/pull/29)
  * Added data restrictions of no more than 90 days from Current date[#30](https://github.com/singer-io/tap-appsflyer/pull/30)

## 0.0.12
  * Adds safety try/catch to bookmarking logic for when `attributed_touch_time` does not exist on `installs` stream rows [#24](https://github.com/singer-io/tap-appsflyer/pull/24)

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
