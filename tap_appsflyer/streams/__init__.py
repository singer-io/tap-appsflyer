from tap_appsflyer.streams.in_app_events import InAppEvents
from tap_appsflyer.streams.installs import Installs
from tap_appsflyer.streams.organic_installs import OrganicInstalls

STREAMS = {
    "installs": Installs,
    "organic_installs": OrganicInstalls,
    "in_app_events": InAppEvents,
}
