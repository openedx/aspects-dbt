
{% docs actor_id %}
The "id" portion of the xAPI Actor for this event. See: https://xapi.com/statements-101/#actor

The LMS can be configured to send different identifiers for an actor, but they are all stored as Strings in ClickHouse: https://docs.openedx.org/projects/openedx-aspects/en/latest/technical_documentation/how-tos/changing_actor_identifier.html

{% enddocs %}

# TODO: link to Opaque Keys docs
{% docs block_id %}
The unique XBlock usage key for this block
{% enddocs %}

# TODO: link to Opaque Keys docs
{% docs course_key %}
The CourseKey for this course run
{% enddocs %}

{% docs course_name %}
The name of the course
{% enddocs %}

{% docs course_order %}
The sort order of this block in the course across all course blocks
{% enddocs %}

{% docs course_run %}
The course run for the course
{% enddocs %}

{% docs display_name %}
The XBlock's display name
{% enddocs %}

{% docs dump_id %}
The UUID of the event sink run that publishes to ClickHouse. When an event is published, all data is sent with the same dump_id.
{% enddocs %}

{% docs email %}
The email address of the learner from the LMS user table. When Aspects PII syncing is off this will be empty.
{% enddocs %}

{% docs emission_time %}
Timestamp, to the second, of when this event was emitted
{% enddocs %}

{% docs enrollment_mode %}
The mode of enrollment such as "audit", "verified", or instance-specific values.
{% enddocs %}

{% docs enrollment_status %}
Whether a learner is actively enrolled in a course
{% enddocs %}

{% docs event_id %}
The unique identifier for the xAPI event, represented as a UUID v5 string, and based upon the actor id, verb, and event timestamp.
{% enddocs %}

{% docs graded %}
Whether the block is graded
{% enddocs %}

{% docs id %}
The primary key value for this row in the edx-platform MySQL database
{% enddocs %}

{% docs learner_name %}
The full name of the learner based on their edx-platform UserProfile "name" field
{% enddocs %}

{% docs object_id %}
The "id" portion of the xAPI Object resource identifier (IRI), stores as a String. See: https://xapi.com/statements-101/#object
{% enddocs %}

{% docs org %}
The organization that the course belongs to, this value is extracted from the course key
{% enddocs %}

{% docs section_block_id %}
The location of this subsection in the course, represented as <section location>:<subsection location>:0
{% enddocs %}

{% docs section_number %}
The location of this section in the course, represented as <section location>:0:0
{% enddocs %}

{% docs section_with_name %}
The name of the section this block belongs to, with section_number prepended
{% enddocs %}

{% docs subsection_block_id %}
The unique identifier for the subsection block
{% enddocs %}

{% docs subsection_number %}
The location of this subsection in the course, represented as <section location>:<subsection location>:0
{% enddocs %}

{% docs subsection_with_name %}
The name of the subsection, with section_number prepended
{% enddocs %}

{% docs time_last_dumped %}
The datetime of the event sink run that publishes to ClickHouse. When an event is published, all data is sent with the same time_last_dumped.
{% enddocs %}

{% docs username %}
The username of the learner from the LMS user table. When Aspects PII syncing is off this will be empty.
{% enddocs %}

{% docs verb_id %}
The "id" portion of the xAPI Verb for this event. See: https://xapi.com/statements-101/#verb
{% enddocs %}

{% docs attempt %}
Number indicating which attempt this was
{% enddocs %}

{% docs problem_number %}
The section, subsection, unit, and part number of the problem block. In the format 1:2:3_1
{% enddocs %}

{% docs success %}
Boolean indicating whether the responses were correct
{% enddocs %}

{% docs object_tag_source %}
Course objects and their associated tags from CMS events:  
`COURSE_CREATED, XBLOCK_CREATED, LIBRARY_BLOCK_CREATED, CONTENT_OBJECT_ASSOCIATIONS_CHANGED`  
and `post_save` Django signal on the Object Tag model
{% enddocs %}
