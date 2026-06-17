PROMPT_TEMPLATE = """You are a statistical dataset analyst.

Analyze the following dataset documentation and metadata, and return a structured JSON object summarizing it.

JSON Format:
{{
  "Dataset Purpose": "<detailed purpose of the dataset and survey goals>",
  "Geographic Coverage": "<geographic areas, states, regions covered>",
  "Population Coverage": "<target population, household criteria, inclusion details>",
  "Key Variables": "<minimum 5 variables with their descriptions/labels, e.g., Land_Owned (Area of land owned), HH_Size (Household size), Sector (Rural/Urban sector), State (State name), Religion (Household religion), ...>",
  "Sample Size / Scope": "<sample size, number of households, individuals, or survey scope>",
  "Important Notes": "<minimum 3 distinct observations, warnings, methodology notes, or weightings, separated by periods or semicolons>"
}}

CRITICAL RULES:
1. The response MUST be a valid JSON object matching the template above. Do not include any preambles, conversational text, markdown wrapping (such as ```json), or extra text outside the JSON object.
2. Every field must contain meaningful, detailed content. Do not leave any field empty.
3. If information is missing for a field, output: "Not explicitly specified in the dataset documentation."
4. For "Key Variables", you must list at least 5 variables. Prefer metadata/DDI labels when available in the documentation.
5. For "Important Notes", you must list at least 3 distinct observations.

Dataset Documentation:
{TEXT}"""
