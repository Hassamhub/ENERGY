"""
Modbus TCP client for Siemens PAC3220 communication
Handles async Modbus TCP connections with proper error handling and data decoding.
"""

import asyncio
import struct
import math
import json
import os
from typing import Optional, Tuple, Any, Dict
from pymodbus.client import ModbusTcpClient  # Use synchronous client
from pymodbus.exceptions import ModbusException, ConnectionException
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian


class ModbusClient:
    """
    Async Modbus TCP client wrapper for PAC3220 communication
    Supports both float (32-bit) and double (64-bit) register reading
    Loads register map from config/register_map.json for configuration-driven operation
    """

    def __init__(self, host: str, port: int = 502, unit_id: int = 1, timeout: float = 10.0):
        self.host = host
        self.port = port
        self.unit_id = unit_id
        self.timeout = timeout
        self.client: Optional[ModbusTcpClient] = None
        self.connected = False

        # Load register map from config file
        self._register_map = {}
        self._parameter_types = {}
        self._load_register_map()

    def _load_register_map(self):
        """Load register map from config/register_map.json"""
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'register_map.json')
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)

            for param_name, param_config in config.items():
                param_type = param_config.get('type')
                address = param_config.get('address')
                scale = param_config.get('scale', 1.0)

                self._register_map[param_name] = {
                    'address': address,
                    'type': param_type,
                    'scale': scale
                }

                # Determine parameter type based on register type
                # input_register typically uses float32, but can be configured
                if param_type == 'input_register':
                    # For energy values, use double (64-bit), for others use float (32-bit)
                    if 'energy' in param_name.lower() or 'kwh' in param_name.lower():
                        self._parameter_types[param_name] = False  # double
                    else:
                        self._parameter_types[param_name] = True   # float
                elif param_type == 'coil':
                    self._parameter_types[param_name] = 'coil'

            print(f"[INFO] Loaded register map with {len(self._register_map)} parameters from {config_path}")

        except FileNotFoundError:
            print(f"[ERROR] Register map config file not found: {config_path}")
            print("[ERROR] Using empty register map - configuration required")
        except json.JSONDecodeError as e:
            print(f"[ERROR] Invalid JSON in register map config: {e}")
        except Exception as e:
            print(f"[ERROR] Failed to load register map: {e}")

    @property
    def REGISTER_MAP(self):
        """Legacy property for backward compatibility - returns address mapping only"""
        return {k: v['address'] for k, v in self._register_map.items()}

    @property
    def PARAMETER_TYPES(self):
        """Legacy property for backward compatibility"""
        return self._parameter_types

    async def connect(self) -> bool:
        """Establish Modbus TCP connection"""
        try:
            self.client = ModbusTcpClient(
                host=self.host,
                port=self.port,
                timeout=self.timeout,
            )

            self.connected = self.client.connect()
            if self.connected:
                print(f"[INFO] Connected to PAC3220 at {self.host}:{self.port} (Unit ID: {self.unit_id})")
            else:
                print(f"[ERROR] Failed to connect to PAC3220 at {self.host}:{self.port} (Unit ID: {self.unit_id})")

            return self.connected

        except Exception as e:
            print(f"[ERROR] Modbus connection error: {str(e).encode('ascii', 'ignore').decode('ascii')}")
            return False

    async def disconnect(self):
        """Close Modbus connection"""
        try:
            if self.client:
                self.client.close()
            self.connected = False
            print(f" Disconnected from {self.host}")
        except Exception as e:
            print(f" Disconnect error: {str(e).encode('ascii', 'replace').decode('ascii')}")
            self.connected = False

    async def _read_registers(self, address: int, count: int) -> Optional[list]:
        """
        Read input registers from Modbus device (PAC3220 uses input registers for measurements)

        Args:
            address: Starting register address (0-based)
            count: Number of registers to read

        Returns:
            List of register values or None if error
        """
        if not self.client or not self.connected:
            print(" Not connected to Modbus device".encode('ascii', 'replace').decode('ascii'))
            return None

        try:
            # Use synchronous read with input registers
            response = self.client.read_input_registers(
                address=address,
                count=count,
                slave=self.unit_id
            )
            if response and not response.isError():
                return response.registers

            print(f"[ERROR] Register read failed at {address}".encode('ascii', 'replace').decode('ascii'))
            return None

        except (ModbusException, ConnectionException, Exception) as e:
            print(f"[ERROR] Modbus read error at address {address}: {str(e).encode('ascii', 'replace').decode('ascii')}")
            # Attempt a lightweight reconnect once
            try:
                if self.client:
                    self.client.close()
                self.connected = self.client.connect()
            except Exception as ex:
                print(f"[ERROR] Reconnect failed: {str(ex).encode('ascii', 'replace').decode('ascii')}")
            return None

    @staticmethod
    def decode_float(registers: list, byte_order: str = ">", word_order: str = "BADC") -> Optional[float]:
        """
        Decode IEEE 754 float from Modbus registers using Siemens PAC3220 byte ordering

        Args:
            registers: List of 2 register values (32-bit float)
            byte_order: Endianness (">" big-endian, "<" little-endian)
            word_order: Word ordering ("ABCD", "BADC", "CDAB", "DCBA")

        Returns:
            Decoded float value or None if invalid
        """
        if not registers or len(registers) != 2:
            return None

        # Reject obvious invalid pattern
        if registers == [0xFFFF, 0xFFFF]:
            return None
        orders = [
            (Endian.Little, Endian.Big),   # common Siemens
            (Endian.Big, Endian.Big),
            (Endian.Little, Endian.Little),
            (Endian.Big, Endian.Little),
        ]
        for b, w in orders:
            try:
                decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=b, wordorder=w)
                val = decoder.decode_32bit_float()
                if val is None or math.isnan(val) or math.isinf(val):
                    continue
                # Guard against absurd values
                if abs(val) > 1e9:
                    continue
                return val
            except Exception:
                continue
        return None

    @staticmethod
    def decode_double(registers: list, byte_order: str = ">", word_order: str = "BADC") -> Optional[float]:
        """
        Decode IEEE 754 double from Modbus registers

        Args:
            registers: List of 4 register values (64-bit double)
            byte_order: Endianness (">" big-endian, "<" little-endian)
            word_order: Word ordering

        Returns:
            Decoded double value or None if invalid
        """
        if not registers or len(registers) != 4:
            return None

        orders = [
            (Endian.Little, Endian.Big),   # common Siemens
            (Endian.Big, Endian.Big),
            (Endian.Little, Endian.Little),
            (Endian.Big, Endian.Little),
        ]
        for b, w in orders:
            try:
                decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=b, wordorder=w)
                val = decoder.decode_64bit_float()
                if val is None or math.isnan(val) or math.isinf(val):
                    continue
                if abs(val) > 1e15:  # above documented overflow
                    continue
                return val
            except Exception:
                continue
        return None

    async def read_float(self, address: int, byte_order: str = ">", word_order: str = "BADC") -> Tuple[Optional[float], Optional[list]]:
        """
        Read and decode a float value from Modbus registers using PAC3220 byte ordering

        Args:
            address: Starting register address
            byte_order: Endianness for decoding (default little-endian for PAC3220)
            word_order: Word ordering for decoding (default "BADC" for PAC3220)

        Returns:
            Tuple of (decoded_value, raw_registers) or (None, None) on error
        """
        registers = await self._read_registers(address, 2)
        if registers is None:
            return None, None

        value = self.decode_float(registers, byte_order, word_order)
        return value, registers

    async def read_double(self, address: int, byte_order: str = ">", word_order: str = "BADC") -> Tuple[Optional[float], Optional[list]]:
        """
        Read and decode a double value from Modbus registers

        Args:
            address: Starting register address
            byte_order: Endianness for decoding
            word_order: Word ordering for decoding

        Returns:
            Tuple of (decoded_value, raw_registers) or (None, None) on error
        """
        registers = await self._read_registers(address, 4)
        if registers is None:
            return None, None

        value = self.decode_double(registers, byte_order, word_order)
        return value, registers

    async def read_parameter(self, param_name: str) -> Tuple[Optional[float], Optional[list]]:
        """
        Read a parameter using the configuration-driven register map

        Args:
            param_name: Parameter name as defined in register_map.json

        Returns:
            Tuple of (scaled_value, raw_registers) or (None, None) on error
        """
        if param_name not in self._register_map:
            print(f"[ERROR] Unknown parameter: {param_name}")
            return None, None

        param_config = self._register_map[param_name]
        address = param_config['address']
        param_type = param_config['type']
        scale = param_config.get('scale', 1.0)

        try:
            if param_type == 'input_register':
                # Determine if float32 or float64 based on parameter type
                is_float32 = self._parameter_types.get(param_name, True)
                if is_float32:
                    value, registers = await self.read_float(address)
                else:
                    value, registers = await self.read_double(address)

                # Apply scaling
                if value is not None:
                    value *= scale

            elif param_type == 'coil':
                # Read coil state using Modbus coils function
                if not self.client or not self.connected:
                    return None, None
                resp = self.client.read_coils(address, 1, slave=self.unit_id)
                if resp and not resp.isError():
                    value = bool(resp.bits[0])
                    registers = [int(resp.bits[0])]
                else:
                    value = None
            else:
                print(f"[ERROR] Unsupported parameter type: {param_type}")
                return None, None

            return value, registers

        except Exception as e:
            print(f"[ERROR] Error reading parameter {param_name}: {e}")
            return None, None

    async def write_coil(self, address: int, value: bool) -> bool:
        """
        Write to a coil (digital output)

        Args:
            address: Coil address
            value: True/False value to write

        Returns:
            True if successful, False otherwise
        """
        if not self.client or not self.connected:
            print("[ERROR] Not connected to Modbus device".encode('ascii', 'replace').decode('ascii'))
            return False

        try:
            response = self.client.write_coil(
                address=address,
                value=value,
                slave=self.unit_id
            )

            if response and not response.isError():
                print(f"✅ Coil {address} set to {value}")
                return True
            else:
                print(f"[ERROR] Failed to write coil {address}".encode('ascii', 'replace').decode('ascii'))
                return False

        except (ModbusException, ConnectionException, Exception) as e:
            print(f"[ERROR] Coil write error at {address}: {str(e).encode('ascii', 'replace').decode('ascii')}")
            return False

    async def read_coil_state(self, address: int) -> Optional[bool]:
        if not self.client or not self.connected:
            return None
        try:
            resp = self.client.read_coils(address, 1, slave=self.unit_id)
            if resp and not resp.isError():
                return bool(resp.bits[0])
            return None
        except (ModbusException, ConnectionException, Exception):
            return None

    async def write_register(self, address: int, value: int) -> bool:
        """
        Write single holding register (FC=06) on PAC3220

        Args:
            address: Zero-based register address
            value: Integer value to write (0/1 for DO)
        """
        if not self.client or not self.connected:
            print("[ERROR] Not connected to Modbus device".encode('ascii', 'replace').decode('ascii'))
            return False
        try:
            response = self.client.write_register(
                address=address,
                value=int(value),
                slave=self.unit_id,
            )
            if response and not response.isError():
                print(f"✅ Register {address} set to {value}")
                return True
            else:
                print(f"[ERROR] Failed to write register {address}".encode('ascii', 'replace').decode('ascii'))
                return False
        except (ModbusException, ConnectionException, Exception) as e:
            print(f"[ERROR] Register write error at {address}: {str(e).encode('ascii', 'replace').decode('ascii')}")
            return False
