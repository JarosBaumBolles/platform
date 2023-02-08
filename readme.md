This is the source code repository for the Hourly Building Data platform

# User documentation
TODO
# Developer documentation

## Conventions
* Cloud function naming template: **[user]-[integration]-[phase]**. Example: **participant0-openweather-fetch**
* Phases glossary:
 * Fetch (*-fetch)
 * Standardize (*-standardize)
 * Load into database (*-db-load)
 * Post-process data in the database (*-db-postprocess)
 * Get data for dashboard visualization (*-db-present)
* Repository:
  * Work flow: Github flow
  * Branch naming convention: [JIRA-ITEM-ID]\_[DEVELOPER]\_[TITLE], example JBB-123\_vyusa\_db\_migration
* Bucket directory structure:
  * /config
  * /[INTEGRATION]/raw
  * /[INTEGRATION]/standardized
* Raw metrics file naming convention: **[ISO8601 polling date]**. Example: **2021-01-01T00:00:00**

## TODO
* Asynchronous or parallel cloud function execution in dispatcher
* Conditional Python requirements to install only subset of packages specific to target cloud function
