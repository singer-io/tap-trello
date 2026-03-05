# Changelog

## 1.2.0
  * Improved 5xx error handling: added `TrelloGatewayTimeoutError` (504), catch-all retry for unmapped 5xx errors via `TrelloBackoffError`, and removed 501 from retry mapping
  * Added unit tests for 504 retry and unmapped 5xx catch-all (520)

## 1.1.1
  * Updates dependency versions for twistlock compliance
  * Updates circle config and tests fix failing build
  * [PR 35](https://github.com/singer-io/tap-trello/pull/35)

## 1.1.0
  * Adds optional config parameter `cards_response_size` and introduces pagination for cards stream
  * Adds unittests for the above
  * [PR 30](https://github.com/singer-io/tap-trello/pull/30)

## 1.0.0
  * Moving tap to general availablity

## 0.1.0
  * Moving tap to public beta
