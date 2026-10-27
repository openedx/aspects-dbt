{{ config(tags=["ccx_identifiers"]) }}

with
    examples as (
        select
            'block-v1:ORG+COURSE+RUN+type@problem+block@question_a' as block_id,
            'question_a' as expected
        union all
        select
            'ccx-block-v1:ORG+COURSE+RUN+ccx@2954+type@problem+block@question_a',
            'question_a'
        union all
        select
            'ccx-block-v1:ORG+COURSE+RUN+ccx@2954+type@problem+block@question_b',
            'question_b'
        union all
        select
            'ccx-block-v1:ORG+COURSE+RUN+ccx@2955+type@problem+block@question_a',
            'question_a'
        union all
        select
            'ccx-block-v1:ORG+COURSE+RUN+ccx@2954+type@sequential+block@section_a',
            'section_a'
        union all
        select 'block-v1:ORG+COURSE+RUN+type@problem+block@question_2_1', 'question_2_1'
    )
-- Replacing the macro with splitByChar('@', block_id)[3] fails the CCX rows.
select block_id, expected, {{ get_block_id_short("block_id") }} as actual
from examples
where actual != expected
