import hashlib

CITY_COORDS = {
    "Ahmedabad": (23.0225, 72.5714), "Aizawl": (23.7271, 92.7176), "Amaravati": (16.5131, 80.5160),
    "Amritsar": (31.6340, 74.8723), "Bengaluru": (12.9716, 77.5946), "Bhopal": (23.2599, 77.4126),
    "Brajrajnagar": (21.8167, 83.9167), "Chandigarh": (30.7333, 76.7794), "Chennai": (13.0827, 80.2707),
    "Coimbatore": (11.0168, 76.9558), "Delhi": (28.6139, 77.2090), "Ernakulam": (9.9816, 76.2999),
    "Gurugram": (28.4595, 77.0266), "Guwahati": (26.1445, 91.7362), "Hyderabad": (17.3850, 78.4867),
    "Jaipur": (26.9124, 75.7873), "Jorapokhar": (23.7000, 86.4167), "Kochi": (9.9312, 76.2673),
    "Kolkata": (22.5726, 88.3639), "Lucknow": (26.8467, 80.9462), "Mumbai": (19.0760, 72.8777),
    "Patna": (25.5941, 85.1376), "Shillong": (25.5788, 91.8933), "Talcher": (20.9500, 85.2167),
    "Thiruvananthapuram": (8.5241, 76.9366), "Visakhapatnam": (17.6868, 83.2185),
}


def station_coords(station_id, city):
    lat, lon = CITY_COORDS.get(city, (22.0, 79.0))
    h = hashlib.md5(station_id.encode()).digest()
    return round(lat + (h[0] / 255 - 0.5) * 0.12, 5), round(lon + (h[1] / 255 - 0.5) * 0.12, 5)