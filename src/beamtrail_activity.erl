-module(beamtrail_activity).

-export([status/1]).

status('activity.scheduled') -> scheduled;
status('activity.started') -> started;
status('activity.succeeded') -> succeeded;
status('activity.failed') -> failed.
