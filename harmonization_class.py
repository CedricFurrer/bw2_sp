from dataclasses import asdict
from pydantic import field_validator
from pydantic.dataclasses import dataclass
from typing import Optional, Callable

@dataclass(frozen = True)
class ActivityDefinition:
    activity_code: Optional[str] = None
    reference_product_code: Optional[str] = None
    activity_name: Optional[str] = None
    reference_product_name: Optional[str] = None
    simapro_name: Optional[str] = None
    location: Optional[str] = None
    unit: Optional[str] = None
    
    @field_validator("activity_code",
                     "reference_product_code",
                     "activity_name",
                     "reference_product_name",
                     "simapro_name",
                     "location",
                     "unit",
                     mode = "before")
    def ensure_string(cls, value) -> (str | None):
        if not isinstance(value, str):
            return None
        return value
    
    @property
    def ID(cls):
        return tuple(cls.__dict__.values())
    
    @property
    def data(cls) -> dict:
        return asdict(cls)

@dataclass(frozen = True)
class Multiplier:
    multiplier: (float | None) = float(1)
    
    @field_validator("_multiplier", mode = "before")
    def is_1_if_None(cls, value: (float | None)) -> float:
        return float(1) if value is None else value



@dataclass(frozen = True)
class ActivityMapping:
    source: ActivityDefinition
    targets: tuple[tuple[ActivityDefinition, Multiplier]]
    
    @field_validator("targets", mode = "before")
    def ensure_multiplier_sum_to_1(cls, value: list) -> None:
        summed: float = sum([m[1].multliplier for m in value])
        if summed != 1:
            raise ValueError("Multiplier {} is either below or above 1".format(summed))


def direct_on_actcode_refcode(act: ActivityDefinition) -> tuple:
    return (act.activity_code, act.reference_product_code)


def direct_on_actname_refname_location_unit(act: ActivityDefinition) -> tuple:
    return (act.activity_name, act.reference_product_name, act.location, act.unit)



class ActivityHarmonization:
    
    def __init__(self):
        self._source_definitions: dict = {}
        self._target_definitions: dict = {}
        self._mappings_dict: dict = {}
        
        self._mapping_type_correspondence: str = "correspondence"
        
        # “rules” are key functions
        self._rule_fns: dict[str, Callable[ActivityDefinition, tuple]] = {
            "direct_on_actname_refname_location_unit": direct_on_actname_refname_location_unit,
            "direct_on_actcode_refcode": direct_on_actcode_refcode,
        }
    
    def add_TO(self,
               source: ActivityDefinition,
               target: dict,
               multiplier: (float | None)
               ) -> None:
        
        self._add_to_mapping(mapping_type = None,
                             source = source,
                             target = target,
                             multiplier = multiplier)
        
    def add_to_correspondence_mapping(self,
                                      source: ActivityDefinition,
                                      target: ActivityDefinition,
                                      multiplier: (float | None)
                                      ) -> None:
        
        self._add_to_mapping(mapping_type = self._mapping_type_correspondence,
                             source = source,
                             target = target,
                             multiplier = multiplier
                             )
        
    def map_using_correspondence(self,
                                 query: ActivityDefinition,
                                 ) -> tuple[tuple[ActivityDefinition]]:
        
        rules: tuple[str] = ("direct_on_actname_refname_location_unit",
                             "direct_on_actcode_refcode"
                             )
        
        founds: tuple[tuple[ActivityDefinition, Multiplier]] = self._map(query = query,
                                                                         mapping_type = self._mapping_type_correspondence,
                                                                         rules = rules
                                                                         )
        
        multipliers: list[float] = [m[1].multiplier for m in founds]

        if sum(multipliers) != 1:
            raise ValueError("Invalid multiplier sum {}. The sum of all multipliers must be exactly 1".format(sum(multipliers)))
        
        final = []
        for query_final, multiplier_final in founds:
            
            final_found: tuple[tuple[dict, Multiplier]] = self._map(query = query_final,
                                                                    mapping_type = None,
                                                                    rules = rules)
            
            if final_found == ():
                return ()
            
            final += [[(dct, mult.multiplier * multiplier_final.multiplier) for dct, mult in final_found]]
            
        return final
            
            
            
    
    def _add_to_mapping(self,
                        mapping_type: str,
                        source: ActivityDefinition,
                        target: (ActivityDefinition | dict),
                        multiplier: (float | None)) -> None:
        
        if mapping_type not in self._source_definitions:
            self._source_definitions[mapping_type]: dict = {}
            
        if mapping_type not in self._target_definitions:
            self._target_definitions[mapping_type]: dict = {}
        
        if source.ID not in self._source_definitions[mapping_type]:
            self._source_definitions[mapping_type][source.ID]: ActivityDefinition = source
            self._target_definitions[mapping_type][source.ID]: tuple[tuple[(ActivityDefinition | dict), Multiplier]] = ((target, Multiplier(multiplier = multiplier)), )
            
        else:
            self._target_definitions[mapping_type][source.ID] += ((target, Multiplier(multiplier = multiplier)),)
        
        # Since we add a new item, we need to reset the mapping dictionary
        self._mappings_dict[mapping_type]: dict = {}
        


    def _get_mapping_dict(self, mapping_type: str, rule: str) -> dict:
        
        if mapping_type not in self._target_definitions:
            return {}
        
        if mapping_type not in self._mappings_dict:
            self._mappings_dict[mapping_type]: dict = {}
        
        if rule in self._mappings_dict[mapping_type]:
            return self._mappings_dict[mapping_type][rule]
        
        else:
            self._mappings_dict[mapping_type][rule]: dict = {}
        
        if rule not in self._rule_fns:
            raise ValueError("Rule '{}' is not implemented".format(rule))
        
        rule_fn = self._rule_fns[rule]
        
        for source_ID, targets in self._target_definitions[mapping_type].items():
            
            source: ActivityDefinition = self._source_definitions[mapping_type][source_ID]
            targets: tuple[tuple[ActivityDefinition, Multiplier]] = self._target_definitions[mapping_type][source_ID]
            rule_tuple: tuple = rule_fn(source)
            
            if not any([n is None for n in rule_tuple]):
                self._mappings_dict[mapping_type][rule] |= {rule_tuple: targets}
        
        return self._mappings_dict[mapping_type][rule]
    

    def _map(self,
             query: ActivityDefinition,
             mapping_type: str,
             rules: tuple[str]
             ) -> tuple[tuple[ActivityDefinition, Multiplier]]:
                
        for rule in rules:
            mapping: dict = self._get_mapping_dict(mapping_type = mapping_type, rule = rule)
            found: tuple[tuple[ActivityDefinition, Multiplier]] = self._get_targets(query = query, rule = rule, mapping = mapping)
            
            if len(found) > 0:
                return found
            
        return ()
        

    def _get_targets(self, query: ActivityDefinition, rule: str, mapping: dict) -> tuple[tuple[ActivityDefinition, Multiplier]]:
        rule_fn = self._rule_fns[rule]
        return mapping.get(rule_fn(query), ())
 

if __name__ == "__main__":
    
    ah: ActivityHarmonization = ActivityHarmonization()
    
    corr_FROM_1 = ActivityDefinition(activity_code = "actcode1",
                                     reference_product_code = "refcode1",
                                     activity_name = "actname1",
                                     reference_product_name = "refname1",
                                     unit = "kilogram",
                                     location = "CH"
                                     )
    
    # print(FROM_1.data)
    
    corr_TO_11 = ActivityDefinition(activity_code = "actcodexxx",
                                    reference_product_code = "refcode0.25",
                                    unit = "meter"
                                    )
        
    corr_multiplier_11 = 0.25
    
    corr_TO_12 = ActivityDefinition(activity_code = "actcode0.75",
                                    reference_product_code = "refcode0.75",
                                    activity_name = "new_name",
                                    unit = "ton"
                                    )
    corr_multiplier_12 = 0.75
    
    
    to_map_FROM_11 = ActivityDefinition(activity_code = "actcodexxx",
                                        reference_product_code = "refcode0.25")
    
    to_map_FROM_12 = ActivityDefinition(activity_code = "actcode0.75",
                                        reference_product_code = "refcode0.75",
                                        unit = "kilogram")
    
    to_map_TO_11_1 = {"worked": "right"}
    to_map_TO_11_2 = {"also": "here"}
    to_map_TO_12_1 = {"and finally": 1}
    
    
    query_1 = ActivityDefinition(activity_code = "actcode1",
                                 reference_product_code = "refcode1",
                                 activity_name = "actname11",
                                 reference_product_name = "refname1",
                                 unit = "kilogram",
                                 location = "CH")
    
    ah.add_to_correspondence_mapping(source = corr_FROM_1, target = corr_TO_11, multiplier = corr_multiplier_11)
    ah.add_to_correspondence_mapping(source = corr_FROM_1, target = corr_TO_12, multiplier = corr_multiplier_12)
    ah.add_TO(source = to_map_FROM_11, target = to_map_TO_11_1, multiplier = 0.48)
    ah.add_TO(source = to_map_FROM_11, target = to_map_TO_11_2, multiplier = 0.52)
    ah.add_TO(source = to_map_FROM_12, target = to_map_TO_12_1, multiplier = 1)


    # ah._get_mapping_dict(mapping_type = "correspondence", rule = "direct_on_actname_refname_location_unit")
    # aaa = ah._mappings_dict
    # for fn, mapping in aaa["correspondence"].items():
    #     for ID, tpl in mapping.items():
    #         for actdef, mult in tpl:
    #             print(mult.multiplier)
        
    result_1: list[tuple[dict, Multiplier]] = ah.map_using_correspondence(query = query_1)
    print(ah._mappings_dict[None])