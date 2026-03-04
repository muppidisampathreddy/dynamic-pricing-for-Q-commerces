import re
import polars as pl
from datetime import datetime

class DataProcessor:
    def _parse_unit(self, unit_str):
        """
        Split unit string and convert kg/l to g/ml:
        - '1.5 kg' -> (1500.0, 'g')
        - '1 ltr' -> (1000.0, 'ml')
        """
        if not unit_str:
            return 0.0, ""
        try:
            unit_str = str(unit_str).lower().strip()
            # Match the first number
            m = re.search(r"(\d+\.?\d*)", unit_str)
            if m:
                val = float(m.group(1))
                # Get the unit part by taking everything after the numeric match
                unit_type = unit_str[m.end():].strip()
                
                # Conversion logic
                if 'kg' in unit_type:
                    val *= 1000
                    unit_type = 'g'
                elif any(x in unit_type for x in ['l', 'ltr', 'liter', 'litre']):
                    # Check if it's ml before converting l
                    if 'ml' not in unit_type:
                        val *= 1000
                        unit_type = 'ml'
                    
                return val, unit_type
            return 0.0, unit_str
        except:
            return 0.0, str(unit_str)

    def process_json(self, json_data, keyword):
        """Flatten Blinkit JSON to Polars DataFrame with normalized units"""
        products = []
        scraped_at = datetime.now()
        
        if not json_data or "response" not in json_data:
            return None

        snippets = json_data["response"].get("snippets", [])
        for snippet in snippets:
            if snippet.get("widget_type") == "product_card_snippet_type_2":
                data = snippet.get("data", {})
                atc_action = data.get("atc_action", {}).get("add_to_cart", {})
                cart_item = atc_action.get("cart_item", {})
                
                if cart_item:
                    unit_str = cart_item.get("unit")
                    uv, ut = self._parse_unit(unit_str)
                    
                    # Extract rating
                    rating = data.get("rating", {}).get("bar", {}).get("value")
                    
                    products.append({
                        "scraped_at": scraped_at,
                        "keyword": keyword,
                        "product_id": cart_item.get("product_id"),
                        "merchant_id": cart_item.get("merchant_id"),
                        "product_name": cart_item.get("product_name"),
                        "brand": cart_item.get("brand"),
                        "price": float(cart_item.get("price", 0)),
                        "mrp": float(cart_item.get("mrp", 0)),
                        "unit_value": uv,
                        "unit_type": ut,
                        "inventory": cart_item.get("inventory"),
                        "unavail_qty": cart_item.get("unavailable_quantity", 0),
                        "rating": float(rating) if rating is not None else None
                    })

        if not products:
            return None

        df = pl.DataFrame(products)
        return df.with_columns([
            pl.col("product_id").cast(pl.Int64),
            pl.col("merchant_id").cast(pl.Int64),
            pl.col("rating").cast(pl.Float64)
        ])
