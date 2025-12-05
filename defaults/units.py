import bw2io

# Add custom normalizations
additional_normalization_mapping: dict = {"guest night": "day"}
additional_transformation_mapping: list[tuple] = [("km*year", "meter-year", 1e3),
                                                  ]

# Create backward unit mappings
# ... import from Brightway2
unit_transformation_mapping: dict = {m[0]: {"unit_transformed": m[1], "multiplier": m[2]} for m in bw2io.units.DEFAULT_UNITS_CONVERSION + additional_transformation_mapping}
backward_unit_normalization_mapping_orig: dict = {v: k for k, v in bw2io.units.UNITS_NORMALIZATION.items()}

# Merge
backward_unit_normalization_mapping: dict = dict(**backward_unit_normalization_mapping_orig, **additional_normalization_mapping)