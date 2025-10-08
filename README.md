# tap-appsflyer

This is a [Singer](https://singer.io) tap that produces JSON-formatted 
data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from AppFlyer's [Raw Data Reports V5 API](https://support.appsflyer.com/hc/en-us/articles/208387843-Raw-Data-Reports-V5-)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Configuration

### Required Parameters

- `app_id` - Your AppsFlyer application ID
- `api_token` - Your AppsFlyer API token

### Optional Parameters

- `start_date` - The date to begin syncing data from (format: `YYYY-MM-DD`)
- `base_url` - Custom base URL for the AppsFlyer API (defaults to `https://hq1.appsflyer.com`)
- `user_agent` - Custom user agent string for API requests
- `organic_installs` - Boolean flag to sync organic installs (defaults to `false`)
- `event_name` - Filter in-app events by specific event names. Can be provided as:
  - A comma-separated string: `"af_purchase,ftd"`
  - An array of strings: `["af_purchase", "ftd"]`

### Example Configuration

```json
{
  "app_id": "com.example.app",
  "api_token": "your-api-token-here",
  "start_date": "2025-01-01",
  "event_name": "af_purchase,ftd"
}
```

Or with event_name as an array:

```json
{
  "app_id": "com.example.app",
  "api_token": "your-api-token-here",
  "start_date": "2025-01-01",
  "event_name": ["af_purchase", "ftd"]
}
```

---

Copyright &copy; 2017 Stitch, Inc.