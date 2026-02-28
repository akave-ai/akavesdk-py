class Size:
    B = 1
    KiB = B << 10
    MiB = KiB << 10
    GiB = MiB << 10
    TiB = GiB << 10
    PiB = TiB << 10
    EiB = PiB << 10

    KB = int(1e3)
    MB = int(1e6)
    GB = int(1e9)
    TB = int(1e12)
    PB = int(1e15)
    EB = int(1e18)

    def __init__(self, size: int):
        self.size = size

    def __str__(self) -> str:
        return self.format_size()

    def to_int(self) -> int:
        return self.size

    def mul_int(self, n: int) -> 'Size':
        return Size(self.size * n)

    def div_int(self, n: int) -> 'Size':
        return Size(self.size // n)

    def format_size(self) -> str:
        if self.size >= self.EB:
            return f"{self.size / self.EB:.2f}EB"
        elif self.size >= self.PB:
            return f"{self.size / self.PB:.2f}PB"
        elif self.size >= self.TB:
            return f"{self.size / self.TB:.2f}TB"
        elif self.size >= self.GB:
            return f"{self.size / self.GB:.2f}GB"
        elif self.size >= self.MB:
            return f"{self.size / self.MB:.2f}MB"
        elif self.size >= self.KB:
            return f"{self.size / self.KB:.2f}KB"
        else:
            return f"{self.size}B"

    @staticmethod
    def format_bytes(bytes_size: int) -> str:
        if bytes_size >= Size.GB:
            return f"{bytes_size // Size.GB} GB"
        elif bytes_size >= Size.MB:
            return f"{bytes_size // Size.MB} MB"
        elif bytes_size >= Size.KB:
            return f"{bytes_size // Size.KB} KB"
        else:
            return f"{bytes_size} Bytes"
