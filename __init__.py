def __init__(self, ip_address: str, port: int = 5683, timeout: float = 5.0):
    super().__init__()  # 🔧 REQUIRED
    self.ip_address = ip_address
    self.port = port
    self.timeout = timeout
    self._context = None
    self._connection_lock = threading.Lock()
    self._sensor_cache: Dict[str, float] = {}
    self._loop = None
