import json
import os
from typing import Dict, Any


class DataSourceIdentifier:    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.data = None
        self.validators = {
            "metrocuadrado": self._is_metrocuadrado_structure,
            "fincaraiz": self._is_fincaraiz_structure,
            "ciencuadras": self._is_ciencuadras_structure
        }
    
    def load_json(self) -> Dict[str, Any]:
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")
        
        with open(self.file_path, 'r', encoding='utf-8') as file:
            self.data = json.load(file)
        
        return self.data
    
    def determine_data_source(self) -> str:
        if self.data is None:
            try:
                self.load_json()
            except (FileNotFoundError, json.JSONDecodeError) as e:
                return f"Error: {e}"
        
        try:
            for source_name, validator in self.validators.items():
                if validator():
                    return source_name
            
            return "invalid source"
        
        except (KeyError, TypeError, AttributeError):
            return "invalid source"
    
    def add_validator(self, source_name: str, validator_func):
        self.validators[source_name] = validator_func
    
    def _is_metrocuadrado_structure(self) -> bool:
        if not self._has_required_fields(self.data, ["responseCode", "message", "data"]):
            return False
        
        # Check data section
        data = self.data.get("data")
        if not isinstance(data, dict):
            return False
        
        if not self._has_required_fields(data, ["metadata", "result"]):
            return False
        
        # Check result section
        result = data.get("result")
        if not isinstance(result, dict):
            return False
        
        if "propertiesByFiltersQuery" not in result:
            return False
        
        # Check properties query section
        properties_query = result.get("propertiesByFiltersQuery")
        if not isinstance(properties_query, dict):
            return False
        
        required_fields = ["count", "total", "properties"]
        if not self._has_required_fields(properties_query, required_fields):
            return False
        
        # Check that properties is a list
        return isinstance(properties_query.get("properties"), list)
    
    def _is_fincaraiz_structure(self) -> bool:
        # Check top-level fields for fincaraiz
        if not self._has_required_fields(self.data, ["took", "timed_out", "_shards", "hits"]):
            return False
        
        # Check _shards section
        shards = self.data.get("_shards")
        if not isinstance(shards, dict):
            return False
        
        # Check hits section
        hits = self.data.get("hits")
        if not isinstance(hits, dict):
            return False
        
        if not self._has_required_fields(hits, ["total", "max_score", "hits"]):
            return False
        
        # Check that hits.hits is a list
        if not isinstance(hits.get("hits"), list):
            return False
        
        # Check for data field (optional but commonly present)
        return True
    
    def _is_ciencuadras_structure(self) -> bool:
        # Check top-level fields for ciencuadras
        if not self._has_required_fields(self.data, ["success", "message", "data"]):
            return False
        
        # Check data section
        data = self.data.get("data")
        if not isinstance(data, dict):
            return False
        
        if not self._has_required_fields(data, ["total", "totalPages", "results"]):
            return False
        
        # Check that results is a list
        if not isinstance(data.get("results"), list):
            return False
        
        # Optional: check for highlights and mapResults (common but not always present)
        return True
    
    def _has_required_fields(self, data: dict, fields: list) -> bool:
        return all(field in data for field in fields)
