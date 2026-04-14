"""Shared test fixtures for the OneLake Security Role Service."""

import pytest


# --- Sample YAML Strings ---

@pytest.fixture
def simple_role_yaml() -> str:
    return """
version: "1.0"
state: present

role:
  name: NeurologyReadRole
  metadata:
    description: "Read access filtered to Neurology"
    owner: "security-team@contoso.com"
  tables:
    - path: /Tables/doctor_table
      columns:
        names: ["*"]
      row_filter: "SELECT * FROM doctor_table WHERE department = 'Neurology'"
"""


@pytest.fixture
def multi_table_role_yaml() -> str:
    return """
version: "1.0"
state: present

role:
  name: GastroRole
  metadata:
    description: "Multi-table with RLS and CLS"
  tables:
    - path: /Tables/doctor_table
      columns:
        names: ["*"]
      row_filter: "SELECT * FROM doctor_table WHERE department = 'Gastroenterology'"
    - path: /Tables/patient_records
      columns:
        names: [patient_id, first_name, last_name, diagnosis_code]
      row_filter: "SELECT * FROM patient_records WHERE attending_dept = 'Gastroenterology'"
"""


@pytest.fixture
def wildcard_role_yaml() -> str:
    return """
version: "1.0"
state: present

role:
  name: FullReadRole
  tables:
    - path: "*"
"""


@pytest.fixture
def schema_level_role_yaml() -> str:
    return """
version: "1.0"
state: present

role:
  name: GoldReportingRole
  tables:
    - path: /Tables/gold_reporting/*
    - path: /Tables/silver_sales/orders
      columns:
        names: [order_id, customer_id, total_amount]
      row_filter: "SELECT * FROM orders WHERE region = 'West'"
"""


@pytest.fixture
def absent_role_yaml() -> str:
    return """
version: "1.0"
state: absent

role:
  name: DeprecatedRole
  metadata:
    description: "Scheduled for removal"
"""


@pytest.fixture
def simple_mapping_yaml() -> str:
    return """
version: "1.0"

mapping:
  role_name: NeurologyReadRole
  members:
    entra_members:
      - object_id: "662b14b6-a936-46b0-bde0-9f4d759dc46e"
        display_name: "Dr. Klerkx"
        object_type: User
      - object_id: "b00395d7-230b-41bb-8bbc-d668979bd8f6"
        display_name: "Dr. Quixada"
        object_type: User
"""


@pytest.fixture
def group_mapping_yaml() -> str:
    return """
version: "1.0"

mapping:
  role_name: GastroRole
  members:
    entra_members:
      - object_id: "6338eaf2-5a87-4eb3-b23b-ea30d04de02a"
        display_name: "Dr. Muir"
        object_type: User
      - object_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        display_name: "Gastro Department SG"
        object_type: Group
"""


@pytest.fixture
def spn_mapping_yaml() -> str:
    return """
version: "1.0"

mapping:
  role_name: FullReadRole
  members:
    entra_members:
      - object_id: "spn-guid-1234"
        display_name: "fabric-etl-spn"
        object_type: ServicePrincipal
"""


# --- Sample API Responses ---

@pytest.fixture
def existing_api_roles() -> list[dict]:
    """Simulates GET /dataAccessRoles response with two existing roles."""
    return [
        {
            "id": "role-id-111",
            "name": "NeurologyReadRole",
            "decisionRules": [
                {
                    "effect": "Permit",
                    "permission": [
                        {"attributeName": "Path", "attributeValueIncludedIn": ["/Tables/doctor_table"]},
                        {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]},
                    ],
                    "constraints": {
                        "columns": [
                            {
                                "tablePath": "/Tables/doctor_table",
                                "columnNames": ["*"],
                                "columnEffect": "Permit",
                                "columnAction": ["Read"],
                            }
                        ],
                        "rows": [
                            {
                                "tablePath": "/Tables/doctor_table",
                                "value": "SELECT * FROM doctor_table WHERE department = 'Neurology'",
                            }
                        ],
                    },
                }
            ],
            "members": {
                "microsoftEntraMembers": [
                    {
                        "tenantId": "tenant-123",
                        "objectId": "662b14b6-a936-46b0-bde0-9f4d759dc46e",
                        "objectType": "User",
                    }
                ],
                "fabricItemMembers": [],
            },
        },
        {
            "id": "role-id-222",
            "name": "OrthoRole",
            "decisionRules": [
                {
                    "effect": "Permit",
                    "permission": [
                        {"attributeName": "Path", "attributeValueIncludedIn": ["/Tables/doctor_table"]},
                        {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]},
                    ],
                }
            ],
            "members": {
                "microsoftEntraMembers": [],
                "fabricItemMembers": [],
            },
        },
    ]


@pytest.fixture
def sample_etag() -> str:
    return '"33a64df551425fcc55e4d42a148795d9f25f89d4"'
