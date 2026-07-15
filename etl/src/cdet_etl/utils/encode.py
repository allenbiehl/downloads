class PandasEncoder(json.JSONEncoder):
    def default(self, obj):
        # Check if the object is a Pandas Timestamp
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return super().default(obj)