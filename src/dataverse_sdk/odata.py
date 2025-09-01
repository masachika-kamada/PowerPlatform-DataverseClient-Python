from __future__ import annotations

from typing import Any, Dict, Optional, List, Union
import re
import json
import uuid

from .http import HttpClient


class ODataClient:
    """Dataverse Web API client: CRUD, SQL-over-API, and table metadata helpers."""

    def __init__(self, auth, base_url: str, config=None) -> None:
        self.auth = auth
        self.base_url = (base_url or "").rstrip("/")
        if not self.base_url:
            raise ValueError("base_url is required.")
        self.api = f"{self.base_url}/api/data/v9.2"
        self.config = config or __import__("dataverse_sdk.config", fromlist=["DataverseConfig"]).DataverseConfig.from_env()
        self._http = HttpClient(
            retries=self.config.http_retries,
            backoff=self.config.http_backoff,
            timeout=self.config.http_timeout,
        )

    def _headers(self) -> Dict[str, str]:
        """Build standard OData headers with bearer auth."""
        scope = f"{self.base_url}/.default"
        token = self.auth.acquire_token(scope).access_token
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }

    def _request(self, method: str, url: str, **kwargs):
        return self._http.request(method, url, **kwargs)

    # ----------------------------- CRUD ---------------------------------
    def create(self, entity_set: str, data: Union[Dict[str, Any], List[Dict[str, Any]], Any], return_ids_only: bool = False) -> Union[Dict[str, Any], List[Dict[str, Any]], str, List[str], Any]:
        """Create one or more records.
        
        Parameters
        ----------
        entity_set : str
            Entity set name (plural logical name).
        data : dict, list of dict, pandas.DataFrame, or pandas.Series
            Single record (dict), list of records, pandas DataFrame, or pandas Series.
        return_ids_only : bool, optional
            If True, return only the GUID(s) instead of full record(s).
            For single records: returns str GUID.
            For multiple records: returns list of str GUIDs.
            
        Returns
        -------
        dict, list of dict, str, list of str, DataFrame, or Series
            - Single dict input: returns dict (or str GUID if return_ids_only=True)
            - List input: returns list of dicts (or list of GUIDs if return_ids_only=True)  
            - DataFrame input: returns DataFrame with results (or Series of GUIDs if return_ids_only=True)
            - Series input: returns dict (or str GUID if return_ids_only=True)
        """
        # Handle pandas DataFrame (duck typing - no pandas import needed)
        if hasattr(data, 'to_dict') and hasattr(data, 'empty'):  # DataFrame-like
            if hasattr(data, 'empty') and data.empty:
                # Return empty DataFrame if possible, otherwise empty list
                if hasattr(data, '__class__'):
                    try:
                        return data.__class__()  # Empty DataFrame
                    except:
                        return []
                return []
            
            records = data.to_dict('records')
            results = self._create_batch(entity_set, records)
            
            if return_ids_only:
                ids = [self._extract_id(record) for record in results]
                # Return as pandas Series if input was DataFrame
                if hasattr(data, 'index') and hasattr(data.__class__, '__name__'):
                    try:
                        # Try to create a Series with the same index
                        Series = getattr(__import__(data.__class__.__module__), 'Series')
                        return Series(ids, index=data.index[:len(ids)])
                    except:
                        return ids
                return ids
            
            # Return as DataFrame if possible
            if hasattr(data.__class__, '__name__'):
                try:
                    DataFrame = getattr(__import__(data.__class__.__module__), 'DataFrame')
                    return DataFrame(results)
                except:
                    pass
            return results
        
        # Handle pandas Series (duck typing)
        elif hasattr(data, 'to_dict') and hasattr(data, 'items'):  # Series-like
            record_dict = data.to_dict()
            result = self._create_single(entity_set, record_dict)
            
            if return_ids_only:
                return self._extract_id(result)
            return result
        
        # Handle single record
        elif isinstance(data, dict):
            result = self._create_single(entity_set, data)
            if return_ids_only:
                return self._extract_id(result)
            return result
        
        # Handle list of records
        elif isinstance(data, list):
            if not data:
                return []
            results = self._create_batch(entity_set, data)
            if return_ids_only:
                return [self._extract_id(record) for record in results]
            return results
        
        raise TypeError(f"Unsupported data type: {type(data)}. Expected dict, list, pandas DataFrame, or pandas Series.")

    def _extract_id(self, record: Dict[str, Any]) -> str:
        """Extract the primary ID from a created record."""
        if not isinstance(record, dict):
            raise RuntimeError("Could not determine created record id from returned representation")
        
        import re
        for k, v in record.items():
            if isinstance(k, str) and k.lower().endswith("id") and isinstance(v, str):
                if re.fullmatch(r"[0-9a-fA-F-]{36}", v.strip() or ""):
                    return v
        raise RuntimeError("Could not determine created record id from returned representation")

    def _create_single(self, entity_set: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a single record."""
        url = f"{self.api}/{entity_set}"
        headers = self._headers().copy()
        headers["Prefer"] = "return=representation"
        r = self._request("post", url, headers=headers, json=data)
        r.raise_for_status()
        return r.json()

    def _create_batch(self, entity_set: str, records: List[Dict[str, Any]], batch_size: int = 25) -> List[Dict[str, Any]]:
        """Create multiple records using batch requests."""
        if not records:
            return []
        
        results = []
        
        # Process records in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_results = self._execute_batch_create(entity_set, batch)
            results.extend(batch_results)
        
        return results

    def _execute_batch_create(self, entity_set: str, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute a single batch request for creating records."""
        batch_id = str(uuid.uuid4())
        changeset_id = str(uuid.uuid4())
        
        # Build batch request body
        batch_body = self._build_batch_body(entity_set, records, batch_id, changeset_id)
        
        # Execute batch request
        url = f"{self.api}/$batch"
        headers = self._headers().copy()
        headers["Content-Type"] = f"multipart/mixed; boundary=batch_{batch_id}"
        
        r = self._request("post", url, headers=headers, data=batch_body)
        r.raise_for_status()
        
        # Parse batch response
        return self._parse_batch_response(r.text, len(records))

    def _build_batch_body(self, entity_set: str, records: List[Dict[str, Any]], batch_id: str, changeset_id: str) -> str:
        """Build the batch request body."""
        lines = []
        
        # Start batch
        lines.append(f"--batch_{batch_id}")
        lines.append(f"Content-Type: multipart/mixed; boundary=changeset_{changeset_id}")
        lines.append("")
        
        # Add each record as a changeset item
        for i, record in enumerate(records):
            lines.append(f"--changeset_{changeset_id}")
            lines.append("Content-Type: application/http")
            lines.append("Content-Transfer-Encoding: binary")
            lines.append(f"Content-ID: {i + 1}")
            lines.append("")
            lines.append(f"POST {self.api}/{entity_set} HTTP/1.1")
            lines.append("Content-Type: application/json")
            lines.append("Prefer: return=representation")
            lines.append("")
            lines.append(json.dumps(record))
            lines.append("")
        
        # End changeset
        lines.append(f"--changeset_{changeset_id}--")
        lines.append("")
        
        # End batch
        lines.append(f"--batch_{batch_id}--")
        
        return "\r\n".join(lines)

    def _parse_batch_response(self, response_text: str, expected_count: int) -> List[Dict[str, Any]]:
        """Parse the batch response and extract created records."""
        results = []
        
        # Split response by HTTP responses
        parts = response_text.split("HTTP/1.1 ")
        
        for part in parts[1:]:  # Skip the first empty part
            lines = part.split("\r\n")
            status_line = lines[0]
            
            # Check if this is a successful creation (201 Created)
            if not status_line.startswith("201"):
                # Handle error - for now, add None for failed records
                results.append(None)
                continue
            
            # Find the JSON response body
            json_start = False
            json_lines = []
            
            for line in lines:
                if json_start:
                    json_lines.append(line)
                elif line.strip() == "":
                    json_start = True
            
            if json_lines:
                try:
                    # Join all JSON lines and parse
                    json_text = "\r\n".join(json_lines).strip()
                    if json_text.startswith("{"):
                        # Find the end of the JSON object
                        json_end = json_text.find("}\r\n--")
                        if json_end != -1:
                            json_text = json_text[:json_end + 1]
                        record = json.loads(json_text)
                        results.append(record)
                    else:
                        results.append(None)
                except json.JSONDecodeError:
                    results.append(None)
            else:
                results.append(None)
        
        # Ensure we return the expected number of results
        while len(results) < expected_count:
            results.append(None)
        
        return results[:expected_count]

    def _format_key(self, key: str) -> str:
        k = key.strip()
        if k.startswith("(") and k.endswith(")"):
            return k
        if len(k) == 36 and "-" in k:
            return f"({k})"
        return f"({k})"

    def update(self, entity_set: str, key: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        headers["Prefer"] = "return=representation"
        r = self._request("patch", url, headers=headers, json=data)
        r.raise_for_status()
        return r.json()

    def delete(self, entity_set: str, key: str) -> None:
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        r = self._request("delete", url, headers=headers)
        r.raise_for_status()

    def get(self, entity_set: str, key: str, select: Optional[str] = None) -> Dict[str, Any]:
        params = {}
        if select:
            params["$select"] = select
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        return r.json()

    # --------------------------- SQL Custom API -------------------------
    def query_sql(self, tsql: str) -> list[dict[str, Any]]:
        payload = {"querytext": tsql}
        headers = self._headers()
        api_name = self.config.sql_api_name
        url = f"{self.api}/{api_name}"
        r = self._request("post", url, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        if "queryresult" not in data:
            raise RuntimeError(f"{api_name} response missing 'queryresult'.")
        q = data["queryresult"]
        if q is None:
            parsed = []
        elif isinstance(q, str):
            s = q.strip()
            parsed = [] if not s else json.loads(s)
        else:
            raise RuntimeError(f"Unexpected queryresult type: {type(q)}")
        return parsed

    # ---------------------- Table metadata helpers ----------------------
    def _label(self, text: str) -> Dict[str, Any]:
        lang = int(self.config.language_code)
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": text,
                    "LanguageCode": lang,
                }
            ],
        }

    def _to_pascal(self, name: str) -> str:
        parts = re.split(r"[^A-Za-z0-9]+", name)
        return "".join(p[:1].upper() + p[1:] for p in parts if p)

    def _get_entity_by_schema(self, schema_name: str) -> Optional[Dict[str, Any]]:
        url = f"{self.api}/EntityDefinitions"
        params = {
            "$select": "MetadataId,LogicalName,SchemaName,EntitySetName",
            "$filter": f"SchemaName eq '{schema_name}'",
        }
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        items = r.json().get("value", [])
        return items[0] if items else None

    def _create_entity(self, schema_name: str, display_name: str, attributes: List[Dict[str, Any]]) -> str:
        url = f"{self.api}/EntityDefinitions"
        payload = {
            "@odata.type": "Microsoft.Dynamics.CRM.EntityMetadata",
            "SchemaName": schema_name,
            "DisplayName": self._label(display_name),
            "DisplayCollectionName": self._label(display_name + "s"),
            "Description": self._label(f"Custom entity for {display_name}"),
            "OwnershipType": "UserOwned",
            "HasActivities": False,
            "HasNotes": True,
            "IsActivity": False,
            "Attributes": attributes,
        }
        headers = self._headers()
        r = self._request("post", url, headers=headers, json=payload)
        r.raise_for_status()
        ent = self._wait_for_entity_ready(schema_name)
        if not ent or not ent.get("EntitySetName"):
            raise RuntimeError(
                f"Failed to create or retrieve entity '{schema_name}' (EntitySetName not available)."
            )
        return ent["MetadataId"]

    def _wait_for_entity_ready(self, schema_name: str, delays: Optional[List[int]] = None) -> Optional[Dict[str, Any]]:
        import time
        delays = delays or [0, 2, 5, 10, 20, 30]
        ent: Optional[Dict[str, Any]] = None
        for idx, delay in enumerate(delays):
            if idx > 0 and delay > 0:
                time.sleep(delay)
            ent = self._get_entity_by_schema(schema_name)
            if ent and ent.get("EntitySetName"):
                return ent
        return ent

    def _attribute_payload(self, schema_name: str, dtype: str, *, is_primary_name: bool = False) -> Optional[Dict[str, Any]]:
        dtype_l = dtype.lower().strip()
        label = schema_name.split("_")[-1]
        if dtype_l in ("string", "text"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                "SchemaName": schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "MaxLength": 200,
                "FormatName": {"Value": "Text"},
                "IsPrimaryName": bool(is_primary_name),
            }
        if dtype_l in ("int", "integer"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.IntegerAttributeMetadata",
                "SchemaName": schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "Format": "None",
                "MinValue": -2147483648,
                "MaxValue": 2147483647,
            }
        if dtype_l in ("decimal", "money"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
                "SchemaName": schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "MinValue": -100000000000.0,
                "MaxValue": 100000000000.0,
                "Precision": 2,
            }
        if dtype_l in ("float", "double"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.DoubleAttributeMetadata",
                "SchemaName": schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "MinValue": -100000000000.0,
                "MaxValue": 100000000000.0,
                "Precision": 2,
            }
        if dtype_l in ("datetime", "date"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
                "SchemaName": schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "Format": "DateOnly",
                "ImeMode": "Inactive",
            }
        if dtype_l in ("bool", "boolean"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.BooleanAttributeMetadata",
                "SchemaName": schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "OptionSet": {
                    "@odata.type": "Microsoft.Dynamics.CRM.BooleanOptionSetMetadata",
                    "TrueOption": {
                        "Value": 1,
                        "Label": self._label("True"),
                    },
                    "FalseOption": {
                        "Value": 0,
                        "Label": self._label("False"),
                    },
                    "IsGlobal": False,
                },
            }
        return None

    def get_table_info(self, tablename: str) -> Optional[Dict[str, Any]]:
        ent = self._get_entity_by_schema(tablename)
        if not ent:
            return None
        return {
            "entity_schema": ent.get("SchemaName") or tablename,
            "entity_logical_name": ent.get("LogicalName"),
            "entity_set_name": ent.get("EntitySetName"),
            "metadata_id": ent.get("MetadataId"),
            "columns_created": [],
        }
    
    def list_tables(self) -> List[Dict[str, Any]]:
        """List all tables in the Dataverse, excluding private tables (IsPrivate=true)."""
        url = f"{self.api}/EntityDefinitions"
        params = {
            "$filter": "IsPrivate eq false"
        }
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        return r.json().get("value", [])

    def delete_table(self, tablename: str) -> None:
        schema_name = tablename if "_" in tablename else f"new_{self._to_pascal(tablename)}"
        entity_schema = schema_name
        ent = self._get_entity_by_schema(entity_schema)
        if not ent or not ent.get("MetadataId"):
            raise RuntimeError(f"Table '{entity_schema}' not found.")
        metadata_id = ent["MetadataId"]
        url = f"{self.api}/EntityDefinitions({metadata_id})"
        headers = self._headers()
        r = self._request("delete", url, headers=headers)
        r.raise_for_status()

    def create_table(self, tablename: str, schema: Dict[str, str]) -> Dict[str, Any]:
        # Accept a friendly name and construct a default schema under 'new_'.
        # If a full SchemaName is passed (contains '_'), use as-is.
        entity_schema = tablename if "_" in tablename else f"new_{self._to_pascal(tablename)}"

        ent = self._get_entity_by_schema(entity_schema)
        if ent:
            raise RuntimeError(f"Table '{entity_schema}' already exists. No update performed.")

        created_cols: List[str] = []
        primary_attr_schema = "new_Name" if "_" not in entity_schema else f"{entity_schema.split('_',1)[0]}_Name"
        attributes: List[Dict[str, Any]] = []
        attributes.append(self._attribute_payload(primary_attr_schema, "string", is_primary_name=True))
        for col_name, dtype in schema.items():
            # Use same publisher prefix segment as entity_schema if present; else default to 'new_'.
            publisher = entity_schema.split("_", 1)[0] if "_" in entity_schema else "new"
            attr_schema = f"{publisher}_{self._to_pascal(col_name)}"
            payload = self._attribute_payload(attr_schema, dtype)
            if not payload:
                raise ValueError(f"Unsupported column type '{dtype}' for '{col_name}'.")
            attributes.append(payload)
            created_cols.append(attr_schema)

        metadata_id = self._create_entity(entity_schema, tablename, attributes)
        ent2: Dict[str, Any] = self._wait_for_entity_ready(entity_schema) or {}
        logical_name = ent2.get("LogicalName")

        return {
            "entity_schema": entity_schema,
            "entity_logical_name": logical_name,
            "entity_set_name": ent2.get("EntitySetName") if ent2 else None,
            "metadata_id": metadata_id,
            "columns_created": created_cols,
        }
