{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override the default schema name generation.
        
        By default, DBT concatenates: <target_schema>_<custom_schema>
        This macro uses the custom_schema directly if provided,
        otherwise falls back to the target schema.
        
        Result:
        - staging models -> STAGING schema
        - marts models -> ANALYTICS schema
        - no custom schema -> ANALYTICS schema (from profile)
    #}
    
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | upper }}
    {%- endif -%}
    
{%- endmacro %}

