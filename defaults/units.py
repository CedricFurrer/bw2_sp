import bw2io

# Create backward unit mappings
# ... import from Brightway2
unit_transformation_mapping: dict = {m[0]: {"unit_transformed": m[1], "multiplier": m[2]} for m in bw2io.units.DEFAULT_UNITS_CONVERSION}
backward_unit_normalization_mapping_orig: dict = {v: k for k, v in bw2io.units.UNITS_NORMALIZATION.items()}

# Add custom normalizations
additional_normalization_mapping: dict = {"guest night": "day"}

# Merge
backward_unit_normalization_mapping: dict = dict(**backward_unit_normalization_mapping_orig, **additional_normalization_mapping)