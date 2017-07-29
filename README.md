# tap-appsflyer

This is a [Singer](https://singer.io) tap that produces JSON-formatted 
data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from AppFlyer's [Raw Data Reports V5 API](https://support.appsflyer.com/hc/en-us/articles/208387843-Raw-Data-Reports-V5-)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

Copyright &copy; 2017 Stitch, Inc.