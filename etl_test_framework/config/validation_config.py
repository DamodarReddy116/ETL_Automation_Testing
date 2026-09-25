CUSTOMER_CONFIG = {
    "source": {
        "type": "csv",
        "path": "data/customer_source.csv",
        "header": True,
        "delimiter": ",",
    },
    "raw": {
        "table": "catalog.raw.customer",
    },
    "operational": {
        "table": "catalog.operational.customer",
    },
    "curated": {
        "table": "catalog.curated.customer_metrics",
    },
    "primary_key": ["customer_id"],
    "mandatory_columns": ["customer_id", "first_name", "last_name", "email", "status"],
    "allowed_status_values": ["ACTIVE", "INACTIVE", "PENDING", "NEW"],
    "advanced_fields": [
        "full_name",
        "email_domain",
        "has_valid_email",
        "signup_month",
        "customer_age_days",
        "customer_tier",
        "risk_flag",
        "customer_segment",
        "is_high_value",
    ],
    "tier_thresholds": {"gold": 500, "silver": 200, "bronze": 0},
    "risk_thresholds": {"high_amount": 1000, "medium_amount": 500},
}

REAL_TIME_TRANSFORM_RULES = {
    "full_name": "trim(concat(first_name, ' ', last_name))",
    "status": "uppercase(trim(status))",
    "amount": "abs(amount) and round to 2 decimal places",
    "is_active": "status in ('ACTIVE', 'PENDING', 'NEW')",
    "business_tier": "tier based on total amount",
    "email_domain": "extract domain name from email",
    "has_valid_email": "regex validation for email format",
    "signup_month": "format signup_date as yyyy-MM",
    "customer_age_days": "datediff(current_date, signup_date)",
    "risk_flag": "HIGH/MEDIUM/LOW based on amount and status",
    "customer_segment": "VIP/LOYAL/REGULAR/NEW based on activity and value",
    "is_high_value": "amount >= 500",
}
