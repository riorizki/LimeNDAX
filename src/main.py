ndax = "/Users/rio.wijaya/Downloads/LimeNDAX/src/LG_2_EOL_test_15-1-4-20250428125158.ndax"

import LimeNDAX as ndax_module

def main():
  barcode = ndax_module.get_barcode(ndax)
  print(f"Barcode: {barcode}")

  process_name = ndax_module.get_process_name(ndax)
  print(f"Process Name: {process_name}")

  remakrs = ndax_module.get_remarks(ndax)
  print(f"Remarks: {remakrs}")

  start_time = ndax_module.get_starttime(ndax)
  print(f"Start Time: {start_time}")

  recipes = ndax_module.get_recipe(ndax)
  print(f"Recipes: {recipes}")

  recipes_v2 = ndax_module.get_recipe_v2(ndax)
  print(f"Recipes V2: {recipes_v2}")

  cycle = ndax_module.get_cycle(ndax)
  print(f"Cycle: {cycle}")

  get_step = ndax_module.get_step(ndax)
  print(f"Step: {get_step}")

  get_records = ndax_module.get_records(ndax, False, False, True)
  print(f"Records: {get_records}")

main()
