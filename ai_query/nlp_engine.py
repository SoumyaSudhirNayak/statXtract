"""
ai_query/nlp_engine.py
Governed Rule-Based NLP Engine for Natural Language Query Parsing.

This engine converts natural language prompts into structured query components
using regex, keyword extraction, and fuzzy matching against DDI metadata.

No external AI/ML APIs or GPU required. Pure CPU-based, lightweight.
"""

import re
from typing import Optional
from difflib import SequenceMatcher


# ─── INTENT KEYWORDS ───
AGGREGATION_KEYWORDS = {
    "average": "AVG", "avg": "AVG", "mean": "AVG",
    "sum": "SUM", "total": "SUM",
    "count": "COUNT", "number of": "COUNT", "how many": "COUNT",
    "minimum": "MIN", "min": "MIN", "lowest": "MIN", "least": "MIN",
    "maximum": "MAX", "max": "MAX", "highest": "MAX", "most": "MAX",
    "median": "MEDIAN",
    "percentage": "PERCENT", "percent": "PERCENT", "rate": "PERCENT",
    "proportion": "PERCENT", "share": "PERCENT",
}

COMPARISON_KEYWORDS = [
    "compare", "comparison", "versus", "vs", "between", "across",
    "difference", "differ", "variation", "trend", "by"
]

STATISTICAL_KEYWORDS = [
    "distribution", "spread", "variance", "standard deviation",
    "correlation", "outlier", "quartile", "percentile",
    "frequency", "histogram"
]

FILTER_PATTERN_KEYWORDS = {
    "male": ("gender", "Male"),
    "female": ("gender", "Female"),
    "transgender": ("gender", "Transgender"),
    "rural": ("sector", "Rural"),
    "urban": ("sector", "Urban"),
    "married": ("marital_status", "Married"),
    "unmarried": ("marital_status", "Unmarried"),
    "single": ("marital_status", "Single"),
    "divorced": ("marital_status", "Divorced"),
    "widowed": ("marital_status", "Widowed"),
    "literate": ("literacy", "Literate"),
    "illiterate": ("literacy", "Illiterate"),
    "employed": ("employment_status", "Employed"),
    "unemployed": ("employment_status", "Unemployed"),
    "self-employed": ("employment_status", "Self-Employed"),
    "self employed": ("employment_status", "Self-Employed"),
    "regular wage": ("employment_type", "Regular Wage"),
    "casual labour": ("employment_type", "Casual Labour"),
    "salaried": ("employment_type", "Regular Wage/Salaried"),
    "hindu": ("religion", "Hindu"),
    "muslim": ("religion", "Muslim"),
    "christian": ("religion", "Christian"),
    "sikh": ("religion", "Sikh"),
    "buddhist": ("religion", "Buddhist"),
    "sc": ("social_group", "SC"),
    "st": ("social_group", "ST"),
    "obc": ("social_group", "OBC"),
    "general": ("social_group", "General"),
    "primary": ("education_level", "Primary"),
    "secondary": ("education_level", "Secondary"),
    "graduate": ("education_level", "Graduate"),
    "post graduate": ("education_level", "Post Graduate"),
    "postgraduate": ("education_level", "Post Graduate"),
    "illiterate education": ("education_level", "Illiterate"),
}

# Indian states for location matching
INDIAN_STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
    "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya",
    "Mizoram", "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim",
    "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand",
    "West Bengal", "Delhi", "Chandigarh", "Puducherry", "Ladakh",
    "Jammu and Kashmir", "Andaman and Nicobar Islands", "Dadra and Nagar Haveli",
    "Daman and Diu", "Lakshadweep"
]

# Common column aliases
COLUMN_ALIASES = {
    "age": ["age", "age_group", "age_years", "person_age"],
    "gender": ["gender", "sex", "person_sex"],
    "income": ["income", "total_income", "monthly_income", "annual_income", "earnings", "wage", "wages", "salary"],
    "education": ["education", "education_level", "educational_attainment", "qualification"],
    "state": ["state", "state_name", "state_code", "region"],
    "district": ["district", "district_name", "district_code"],
    "sector": ["sector", "area_type", "rural_urban"],
    "occupation": ["occupation", "occupation_code", "industry", "nco_code", "nic_code"],
    "religion": ["religion", "religious_group"],
    "social_group": ["social_group", "caste", "caste_category"],
    "marital_status": ["marital_status", "marital"],
    "employment": ["employment_status", "employment_type", "work_status", "activity_status"],
    "expenditure": ["expenditure", "consumption", "spending", "monthly_expenditure", "mpce"],
    "household": ["household_size", "household_type", "hh_size", "hh_type"],
    "literacy": ["literacy", "literacy_status", "literate"],
}


def fuzzy_match(query_word: str, candidates: list[str], threshold: float = 0.65) -> Optional[str]:
    """Find the best fuzzy match for a word in a list of candidates."""
    best = None
    best_score = 0.0
    query_lower = query_word.lower()
    for c in candidates:
        score = SequenceMatcher(None, query_lower, c.lower()).ratio()
        if score > best_score and score >= threshold:
            best = c
            best_score = score
    return best


def extract_age_range(text: str) -> Optional[dict]:
    """Extract age range filters from text like '20-30', 'aged 20 to 30', 'above 60'."""
    # Pattern: "20-30" or "20 to 30"
    m = re.search(r'(\d{1,3})\s*[-–to]+\s*(\d{1,3})', text, re.IGNORECASE)
    if m:
        low, high = int(m.group(1)), int(m.group(2))
        if 0 <= low <= 120 and 0 <= high <= 120:
            return {"type": "BETWEEN", "low": low, "high": high}
    
    # Pattern: "above 60", "over 50", "more than 40"
    m = re.search(r'(?:above|over|more than|greater than|older than|>\s*)\s*(\d{1,3})', text, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        if 0 <= val <= 120:
            return {"type": ">=", "value": val}
    
    # Pattern: "below 20", "under 18", "less than 30"
    m = re.search(r'(?:below|under|less than|younger than|<\s*)\s*(\d{1,3})', text, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        if 0 <= val <= 120:
            return {"type": "<=", "value": val}
    
    # Pattern: "aged 25", "age 30"
    m = re.search(r'(?:aged?)\s*(\d{1,3})(?!\s*[-–to])', text, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        if 0 <= val <= 120:
            return {"type": "=", "value": val}
    
    return None


def extract_limit(text: str) -> Optional[int]:
    """Extract row limit from text like 'top 10', 'first 50', 'limit 100', 'list 15 records'."""
    # Pattern 1: keyword followed by number (e.g. list 15, top 10, limit 100)
    m = re.search(r'\b(?:top|first|limit|show|list)\s*(\d+)\b', text, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        if 1 <= val <= 10000:
            return val
    # Pattern 2: number followed by row/record/line/result keyword (e.g. 15 records, 20 rows)
    m2 = re.search(r'\b(\d+)\s*(?:records|rows|results|lines|items)\b', text, re.IGNORECASE)
    if m2:
        val = int(m2.group(1))
        if 1 <= val <= 10000:
            return val
    return None


class NLPQueryEngine:
    """
    Rule-based NLP engine that parses natural language queries into structured
    query components that can be validated by the governance system.
    """
    
    def __init__(self, available_columns: list[str] = None, variable_configs: dict = None):
        """
        Args:
            available_columns: List of actual column names in the target table
            variable_configs: Dict of variable configurations from privacy_guard
        """
        self.available_columns = [c.lower() for c in (available_columns or [])]
        self.available_columns_orig = available_columns or []
        self.variable_configs = variable_configs or {}
    
    def _find_column(self, keyword: str) -> Optional[str]:
        """Find the actual column name matching a keyword or alias."""
        kw = keyword.lower().strip()
        
        # Direct match
        for col in self.available_columns_orig:
            if col.lower() == kw:
                return col
        
        # Check aliases
        for alias_key, alias_list in COLUMN_ALIASES.items():
            if kw in alias_list or kw == alias_key:
                # Find matching column in available columns
                for alias in alias_list:
                    for col in self.available_columns_orig:
                        if col.lower() == alias.lower():
                            return col
        
        # Fuzzy match
        match = fuzzy_match(kw, self.available_columns_orig, threshold=0.7)
        if match:
            return match
        
        return None
    
    def parse(self, prompt: str) -> dict:
        """
        Parse a natural language prompt into structured query components.
        
        Returns:
            {
                "original_prompt": str,
                "intent": str,            # exploratory|statistical|aggregation|comparison|sensitive
                "filters": [...],          # list of {column, operator, value}
                "aggregations": [...],     # list of {function, column}
                "group_by": [...],         # list of column names
                "select_columns": [...],   # columns to select
                "order_by": str|None,
                "limit": int,
                "confidence": float,       # 0.0 - 1.0
                "interpretation": str,     # human-readable interpretation
                "warnings": [...],
            }
        """
        result = {
            "original_prompt": prompt,
            "intent": "exploratory",
            "filters": [],
            "aggregations": [],
            "group_by": [],
            "select_columns": [],
            "order_by": None,
            "limit": 100,
            "confidence": 0.0,
            "interpretation": "",
            "warnings": [],
        }
        
        if not prompt or not prompt.strip():
            result["warnings"].append("Empty prompt provided")
            return result
        
        prompt_lower = prompt.lower().strip()
        confidence_score = 0.0
        interpretation_parts = []
        
        # ─── 1. EXTRACT AGGREGATIONS ───
        for keyword, func in AGGREGATION_KEYWORDS.items():
            if keyword in prompt_lower:
                # Try to find what column to aggregate
                # Pattern: "average income", "count of workers", "sum salary"
                pattern = rf'{re.escape(keyword)}\s+(?:of\s+)?(\w+)'
                m = re.search(pattern, prompt_lower)
                if m:
                    target_word = m.group(1)
                    col = self._find_column(target_word)
                    if col:
                        result["aggregations"].append({"function": func, "column": col})
                        interpretation_parts.append(f"{func}({col})")
                        confidence_score += 0.2
                    else:
                        result["aggregations"].append({"function": func, "column": "*"})
                        interpretation_parts.append(f"{func}(*)")
                        confidence_score += 0.1
                else:
                    result["aggregations"].append({"function": func, "column": "*"})
                    interpretation_parts.append(f"{func}(*)")
                    confidence_score += 0.1
                break  # Take first aggregation match
        
        # ─── 2. EXTRACT KEYWORD FILTERS ───
        for keyword, (col_alias, value) in FILTER_PATTERN_KEYWORDS.items():
            if keyword in prompt_lower:
                col = self._find_column(col_alias)
                if col:
                    result["filters"].append({
                        "column": col,
                        "operator": "=",
                        "value": value
                    })
                    interpretation_parts.append(f"{col} = '{value}'")
                    confidence_score += 0.15
                else:
                    # Still record the intent even if column not found
                    result["filters"].append({
                        "column": col_alias,
                        "operator": "=",
                        "value": value
                    })
                    interpretation_parts.append(f"{col_alias} = '{value}' (column not confirmed)")
                    result["warnings"].append(f"Column '{col_alias}' not found in table - filter may not apply")
                    confidence_score += 0.05
        
        # ─── 3. EXTRACT STATE/LOCATION FILTERS ───
        for state in INDIAN_STATES:
            if state.lower() in prompt_lower:
                col = self._find_column("state") or self._find_column("state_name")
                if col:
                    result["filters"].append({
                        "column": col,
                        "operator": "=",
                        "value": state
                    })
                    interpretation_parts.append(f"{col} = '{state}'")
                    confidence_score += 0.15
                else:
                    result["filters"].append({
                        "column": "state",
                        "operator": "=",
                        "value": state
                    })
                    interpretation_parts.append(f"state = '{state}' (column not confirmed)")
                    confidence_score += 0.05
                break  # Take first state match
        
        # ─── 4. EXTRACT AGE RANGE ───
        age_range = extract_age_range(prompt)
        if age_range:
            col = self._find_column("age") or self._find_column("age_group")
            if col:
                if age_range["type"] == "BETWEEN":
                    result["filters"].append({
                        "column": col,
                        "operator": "BETWEEN",
                        "value": f"{age_range['low']} AND {age_range['high']}"
                    })
                    interpretation_parts.append(f"{col} BETWEEN {age_range['low']} AND {age_range['high']}")
                else:
                    result["filters"].append({
                        "column": col,
                        "operator": age_range["type"],
                        "value": str(age_range.get("value", ""))
                    })
                    interpretation_parts.append(f"{col} {age_range['type']} {age_range.get('value', '')}")
                confidence_score += 0.15
        
        # ─── 5. EXTRACT LIMIT ───
        limit = extract_limit(prompt)
        if limit:
            result["limit"] = limit
        
        # ─── 6. CLASSIFY INTENT ───
        if any(kw in prompt_lower for kw in COMPARISON_KEYWORDS):
            result["intent"] = "comparison"
            confidence_score += 0.1
            # Try to detect group_by columns from "across states", "by gender"
            for pattern in [r'(?:across|by|per|for each)\s+(\w+)', r'(?:compare)\s+(\w+)']:
                m = re.search(pattern, prompt_lower)
                if m:
                    gb_word = m.group(1)
                    gb_col = self._find_column(gb_word)
                    if gb_col and gb_col not in result["group_by"]:
                        result["group_by"].append(gb_col)
                        interpretation_parts.append(f"GROUP BY {gb_col}")
        
        if result["aggregations"]:
            if result["intent"] != "comparison":
                result["intent"] = "aggregation"
        elif any(kw in prompt_lower for kw in STATISTICAL_KEYWORDS):
            result["intent"] = "statistical"
            confidence_score += 0.1
        
        # ─── 7. DETECT SELECT COLUMNS ───
        # If "show" is used, try to identify what columns to show
        show_match = re.search(r'(?:show|display|list|get)\s+(.+?)(?:\s+(?:from|where|for|in|of)\b|$)', prompt_lower)
        if show_match and not result["aggregations"]:
            tokens = show_match.group(1).split()
            for token in tokens:
                col = self._find_column(token)
                if col and col not in result["select_columns"]:
                    result["select_columns"].append(col)
        
        # ─── 8. CALCULATE FINAL CONFIDENCE ───
        confidence_score = min(1.0, confidence_score)
        if not result["filters"] and not result["aggregations"]:
            confidence_score = max(0.1, confidence_score)
            result["warnings"].append("Could not extract specific filters or aggregations from the prompt")
        
        result["confidence"] = round(confidence_score, 2)
        
        # ─── 9. BUILD INTERPRETATION ───
        if interpretation_parts:
            result["interpretation"] = " | ".join(interpretation_parts)
        else:
            result["interpretation"] = "General data exploration query"
        
        return result


def classify_query_risk(parsed: dict, variable_configs: dict = None) -> dict:
    """
    Classify the risk level of a parsed AI query.
    
    Risk factors:
    - Excessive filtering (identity targeting)
    - Sensitive variable targeting
    - Very small group extraction
    - Multiple identity-type filters combined
    
    Returns: {"level": "low"|"medium"|"high", "score": int, "factors": [...]}
    """
    score = 0
    factors = []
    
    identity_columns = {"gender", "religion", "caste", "social_group", "marital_status", "age"}
    sensitive_columns = set()
    
    if variable_configs:
        for col, cfg in variable_configs.items():
            if cfg.get("is_sensitive"):
                sensitive_columns.add(col.lower())
    
    # Factor 1: Number of filters (excessive narrowing)
    filter_count = len(parsed.get("filters", []))
    if filter_count >= 5:
        score += 40
        factors.append(f"Excessive filtering: {filter_count} filters applied (identity targeting risk)")
    elif filter_count >= 3:
        score += 20
        factors.append(f"Multiple filters: {filter_count} filters applied")
    
    # Factor 2: Identity-type column filtering
    identity_filter_count = 0
    for f in parsed.get("filters", []):
        col_lower = f.get("column", "").lower()
        if col_lower in identity_columns or any(id_col in col_lower for id_col in identity_columns):
            identity_filter_count += 1
    
    if identity_filter_count >= 3:
        score += 35
        factors.append(f"Multiple identity-type filters ({identity_filter_count}) - potential re-identification risk")
    elif identity_filter_count >= 2:
        score += 15
        factors.append(f"Identity-type filtering on {identity_filter_count} columns")
    
    # Factor 3: Sensitive variable targeting
    for f in parsed.get("filters", []):
        if f.get("column", "").lower() in sensitive_columns:
            score += 25
            factors.append(f"Sensitive variable targeted: {f['column']}")
    
    for agg in parsed.get("aggregations", []):
        if agg.get("column", "").lower() in sensitive_columns:
            score += 20
            factors.append(f"Aggregation on sensitive variable: {agg['column']}")
    
    # Factor 4: Very narrow age range
    for f in parsed.get("filters", []):
        if "age" in f.get("column", "").lower() and f.get("operator") == "BETWEEN":
            try:
                parts = f["value"].split("AND")
                if len(parts) == 2:
                    low, high = int(parts[0].strip()), int(parts[1].strip())
                    if (high - low) <= 3:
                        score += 20
                        factors.append(f"Very narrow age range: {low}-{high} (small group extraction risk)")
            except (ValueError, IndexError):
                pass
    
    # Factor 5: Low limit (trying to extract individual records)
    if parsed.get("limit", 100) <= 5:
        score += 15
        factors.append(f"Very low result limit: {parsed['limit']} rows")
    
    # Determine level
    if score >= 60:
        level = "high"
    elif score >= 30:
        level = "medium"
    else:
        level = "low"
    
    return {
        "level": level,
        "score": min(100, score),
        "factors": factors
    }


def generate_summary(parsed: dict, results: list, columns: list) -> str:
    """
    Generate a simple template-based statistical summary from query results.
    No external AI required - pure rule-based.
    """
    if not results:
        return "No data found matching the specified criteria."
    
    row_count = len(results)
    parts = []
    
    # Basic count
    parts.append(f"Found {row_count} record{'s' if row_count != 1 else ''}")
    
    # Describe applied filters
    filter_descs = []
    for f in parsed.get("filters", []):
        if f["operator"] == "BETWEEN":
            filter_descs.append(f"{f['column']} between {f['value']}")
        elif f["operator"] == "=":
            filter_descs.append(f"{f['column']} = {f['value']}")
        else:
            filter_descs.append(f"{f['column']} {f['operator']} {f['value']}")
    
    if filter_descs:
        parts.append(f"matching filters: {', '.join(filter_descs)}")
    
    # If aggregation results, describe them
    for agg in parsed.get("aggregations", []):
        col = agg["column"]
        func = agg["function"]
        if results and col != "*":
            try:
                values = [float(r.get(col, 0) or 0) for r in results if r.get(col) is not None]
                if values:
                    if func == "AVG":
                        avg_val = sum(values) / len(values)
                        parts.append(f"The average {col} is {avg_val:,.2f}")
                    elif func == "SUM":
                        parts.append(f"The total {col} is {sum(values):,.2f}")
                    elif func == "COUNT":
                        parts.append(f"Total count: {len(values)}")
                    elif func == "MIN":
                        parts.append(f"Minimum {col}: {min(values):,.2f}")
                    elif func == "MAX":
                        parts.append(f"Maximum {col}: {max(values):,.2f}")
            except (ValueError, TypeError):
                pass
    
    # If no aggregation, try to provide basic stats on numeric columns
    if not parsed.get("aggregations") and results:
        numeric_cols = []
        for col in columns[:5]:
            try:
                vals = [float(r.get(col, 0) or 0) for r in results[:50] if r.get(col) is not None]
                if vals and len(vals) > 1:
                    numeric_cols.append((col, vals))
            except (ValueError, TypeError):
                pass
        
        if numeric_cols:
            col, vals = numeric_cols[0]
            avg = sum(vals) / len(vals)
            parts.append(f"Average {col}: {avg:,.2f} (range: {min(vals):,.2f} - {max(vals):,.2f})")
    
    # Group-by summary
    if parsed.get("group_by") and results:
        for gb_col in parsed["group_by"][:1]:
            unique_vals = set()
            for r in results:
                v = r.get(gb_col)
                if v is not None:
                    unique_vals.add(str(v))
            if unique_vals:
                parts.append(f"Data spans {len(unique_vals)} unique {gb_col} value{'s' if len(unique_vals) > 1 else ''}")
    
    summary = ". ".join(parts) + "."
    return summary
