-- Adding logic to set to default schema when schema env var is blank

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = custom_schema_name -%}

    {%- if target.schema == 'prod' -%}
        {{ default_schema | trim }}
        {{ log("Setting Schema: {0}".format(default_schema | trim)) }}
    {%- else -%}
        {{ target.schema }}_{{ default_schema | trim }}
        {{ log("Setting Prefix Schema: {0}".format(target.schema)) }}
    {%- endif -%}

{%- endmacro %}
 