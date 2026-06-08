-module(beamtrail_effect).

-export([call_step/6,
         call_step_id/2,
         id/1,
         type/1,
         workflow/1,
         step_id/1,
         step_version/1,
         input/1,
         timeout_ms/1,
         context/1,
         idempotency_key/1]).

call_step(Workflow, StepId, StepVersion, Input, TimeoutMs, Context) ->
    AttemptNo = maps:get(attempt, Context),
    #{effect_type => call_step,
      effect_id => call_step_id(StepId, AttemptNo),
      workflow => Workflow,
      step_id => StepId,
      step_version => StepVersion,
      input => Input,
      timeout_ms => TimeoutMs,
      context => Context}.

call_step_id(StepId, AttemptNo) ->
    {call_step, StepId, AttemptNo}.

id(#{effect_id := EffectId}) ->
    EffectId.

type(#{effect_type := Type}) ->
    Type.

workflow(#{workflow := Workflow}) ->
    Workflow.

step_id(#{step_id := StepId}) ->
    StepId.

step_version(#{step_version := StepVersion}) ->
    StepVersion.

input(#{input := Input}) ->
    Input.

timeout_ms(#{timeout_ms := TimeoutMs}) ->
    TimeoutMs.

context(#{context := Context}) ->
    Context.

idempotency_key(Effect) ->
    maps:get(idempotency_key, context(Effect)).
