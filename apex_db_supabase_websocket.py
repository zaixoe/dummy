import os
import sys
import time
import hmac
import hashlib
import requests
import threading
import traceback
import telegram
import ssl
import numpy as np
import tulipy as ti
import json
import math
import logging
import psycopg2
import websocket
from threading import Thread, Event
from queue import Queue
from datetime import datetime, timedelta
from urllib.parse import urlencode
from dotenv import load_dotenv
from flask import Flask
from collections import deque

# --- Tambahan untuk Database ---
from psycopg2 import sql
from psycopg2.extras import Json

# --- [0] Setup Aplikasi Web Flask & Logging ---
app = Flask(__name__)
load_dotenv()

# Konfigurasi Logging yang Robust
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_activity.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- [1] KONFIGURASI BOT ---
INDODAX_API_KEY = os.getenv("INDODAX_API_KEY")
INDODAX_SECRET_KEY = os.getenv("INDODAX_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
PERSISTENCE_FILE = "bot_state.json" # File untuk menyimpan state bot 
BOT_PUBLIC_URL = os.getenv("BOT_PUBLIC_URL") # URL publik bot Anda, misal: https://namabotanda.onrender.com/
SELF_PING_INTERVAL_SECONDS = 10 * 60
# Parameter Database
DATABASE_URL = os.getenv("DATABASE_URL") # URL koneksi PostgreSQL

MAX_SPREAD_PERCENT = 1.0 # Maksimal persentase spread yang diizinkan (misal: 1.0% = 1 persen)

# Parameter Strategi
MAKSIMAL_KOIN_DITRADING = 3
TOTAL_RISK_PER_EQUITY_PERCENT = 1.5 # Total risiko ekuitas per trade
MINIMUM_INVESTMENT_IDR = 20000 # Minimum investasi per trade
MINIMUM_VOLUME_24H_IDR = 1_000_000_000 # Minimum volume 24h agar koin dipertimbangkan
BLACKLIST_KOIN = ['usdt_idr', 'usdc_idr', 'busd_idr', 'dai_idr']
MAX_SPREAD_PERCENT = 1.0

# Parameter Indikator
CANDLE_INTERVAL_MINUTES = 15 # Interval lilin sinyal yang diinginkan (dalam menit)
SMA_FAST_PERIOD = 20
SMA_SLOW_PERIOD = 50
SMA_FILTER_PERIOD = 200
ADX_PERIOD = 14
MINIMUM_ADX_VALUE = 25
ATR_PERIOD_STOP_LOSS = 14
ATR_FACTOR_STOP_LOSS = 2.5

# Parameter Pyramiding
USE_PYRAMIDING = True
MAX_PYRAMID_BUYS_PER_COIN = 3
PYRAMID_TARGET_PROFIT_PERCENT = 3.0
PYRAMID_SCALE_FACTOR = 0.75 # Persentase dari modal_awal untuk pembelian pyramid

# Parameter Pelaporan
REPORT_INTERVAL_SECONDS = 86400 # Laporan portofolio harian (24 jam)
RADAR_REPORT_INTERVAL_HOURS = 2 # Laporan radar pasar (2 jam)

# --- [2] FUNGSI BANTU & KESALAHAN ---

class IndodaxAPIError(Exception):
    """Custom exception for Indodax API errors."""
    pass

def indodax_request(method, params, api_key, secret_key):
    """
    Melakukan permintaan ke Indodax TAPI (Trade API) dengan signing HMAC-SHA512.
    Meningkatkan robustness error handling.
    """
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 5 # Jeda 5 detik jika kena rate limit

    for attempt in range(MAX_RETRIES):
        try:
            nonce = int(time.time() * 1000)
            params['method'] = method
            params['timestamp'] = nonce
            
            sorted_params = sorted(params.items())
            data = urlencode(sorted_params).encode()
            
            signature = hmac.new(secret_key.encode(), data, hashlib.sha512).hexdigest()
            headers = {
                'Key': api_key,
                'Sign': signature,
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            response = requests.post("https://indodax.com/tapi", headers=headers, data=data, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            if result.get('success') != 1:
                error_msg = result.get('error', 'Unknown Indodax API error')
                logger.error(f"Indodax API returned error for method '{method}': {error_msg}. Params: {params}")
                
                # --- PENANGANAN RATE LIMIT ---
                if "too many requests" in error_msg.lower() or "limit" in error_msg.lower():
                    logger.warning(f"Rate limit hit for method '{method}'. Retrying in {RETRY_DELAY_SECONDS} seconds (Attempt {attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue # Coba lagi
                # --- AKHIR PENANGANAN RATE LIMIT ---
                
                raise IndodaxAPIError(error_msg)
                
            return result
        except requests.exceptions.Timeout:
            logger.error(f"Indodax API request timed out for method '{method}'. Params: {params}")
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"Timeout for '{method}'. Retrying in {RETRY_DELAY_SECONDS} seconds (Attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            raise IndodaxAPIError("API request timed out after multiple retries")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Indodax API HTTP error for method '{method}': {e}. Response: {e.response.text}. Params: {params}")
            if e.response.status_code == 429: # HTTP status code for Too Many Requests
                 logger.warning(f"Rate limit hit (HTTP 429) for '{method}'. Retrying in {RETRY_DELAY_SECONDS} seconds (Attempt {attempt + 1}/{MAX_RETRIES})...")
                 time.sleep(RETRY_DELAY_SECONDS)
                 continue
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"HTTP error for '{method}'. Retrying in {RETRY_DELAY_SECONDS} seconds (Attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            raise IndodaxAPIError(f"HTTP error: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Indodax API request general error for method '{method}': {e}. Params: {params}")
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"General request error for '{method}'. Retrying in {RETRY_DELAY_SECONDS} seconds (Attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            raise IndodaxAPIError(f"Request error: {e} after multiple retries")
        except json.JSONDecodeError:
            logger.error(f"Indodax API returned invalid JSON for method '{method}'. Response: {response.text}. Params: {params}")
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"Invalid JSON for '{method}'. Retrying in {RETRY_DELAY_SECONDS} seconds (Attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            raise IndodaxAPIError("Invalid JSON response from API after multiple retries")
        except Exception as e:
            logger.error(f"Unexpected error in indodax_request for method '{method}': {e}. Trace: {traceback.format_exc()}")
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"Unexpected error for '{method}'. Retrying in {RETRY_DELAY_SECONDS} seconds (Attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            raise IndodaxAPIError(f"Unexpected error: {e} after multiple retries")
    
    # Jika semua percobaan gagal
    raise IndodaxAPIError(f"Failed to execute Indodax API request '{method}' after {MAX_RETRIES} attempts.")
            

def get_indodax_public_summaries():
    """
    Mengambil ringkasan pasar publik dari Indodax.
    Menambahkan validasi data yang lebih ketat.
    """
    try:
        url = "https://indodax.com/api/summaries"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json().get('tickers', {})
        
        qualified_pairs = {}
        for pair, info in data.items():
            try:
                if not pair.endswith('_idr'):
                    continue
                if pair in BLACKLIST_KOIN:
                    continue
                
                # Validasi dan konversi semua nilai yang dibutuhkan ke float
                # Memberikan nilai default 0.0 jika kunci tidak ada atau tidak valid
                vol_idr_str = info.get('vol_idr')
                last_price_str = info.get('last')
                high_price_str = info.get('high')
                low_price_str = info.get('low')
                
                if not (vol_idr_str and last_price_str and high_price_str and low_price_str):
                    # logger.debug(f"Skipping pair {pair}: missing data.")
                    continue

                volume_idr = float(vol_idr_str)
                last_price = float(last_price_str)
                high_price = float(high_price_str)
                low_price = float(low_price_str)

                if volume_idr >= MINIMUM_VOLUME_24H_IDR:
                    # Validasi buy/sell prices, default ke last price jika tidak ada
                    buy_price = float(info.get('buy', last_price_str))
                    sell_price = float(info.get('sell', last_price_str))

                    # Tambahkan filter spread di sini juga (opsional, bisa juga di find_new_opportunities)
                    if buy_price > 0 and sell_price > 0:
                        spread_percent = ((sell_price - buy_price) / buy_price) * 100
                        if spread_percent > MAX_SPREAD_PERCENT:
                            logger.debug(f"Skipping pair {pair} due to high spread: {spread_percent:.2f}% > {MAX_SPREAD_PERCENT:.2f}%")
                            continue

                    qualified_pairs[pair] = {
                        'vol_idr': volume_idr,
                        'last': last_price,
                        'high': high_price,
                        'low': low_price,
                        'buy': buy_price,
                        'sell': sell_price
                    }
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping pair {pair} due to invalid number format in summaries: {e}")
                continue
        
        logger.info(f"Found {len(qualified_pairs)} coins meeting volume criteria from summaries.")
        return qualified_pairs
    except requests.exceptions.Timeout:
        logger.error("Public summaries request timed out.")
        return {}
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Indodax public summaries: {e}")
        return {}
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON response from Indodax public summaries. Response: {response.text}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error in get_indodax_public_summaries: {e}. Trace: {traceback.format_exc()}")
        return {}

def kirim_notifikasi_telegram(pesan):
    """Mengirim pesan ke channel Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        logger.warning("Telegram bot token or channel ID not configured. Skipping notification.")
        return False
    try:
        bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
        bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=pesan, parse_mode='Markdown')
        logger.info("Telegram notification sent.")
        return True
    except telegram.error.TelegramError as e:
        logger.error(f"Telegram API error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending Telegram notification: {e}. Trace: {traceback.format_exc()}")
        return False

# --- FUNGSI BANTU DATABASE ---
def get_db_connection():
    """Mendapatkan koneksi ke database PostgreSQL."""
    if not DATABASE_URL:
        logger.error("DATABASE_URL tidak ditemukan. Koneksi database gagal.")
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.debug("Koneksi database berhasil.")
        return conn
    except Exception as e:
        logger.error(f"Gagal terhubung ke database: {e}. Trace: {traceback.format_exc()}")
        return None

def initialize_database():
    """Menginisialisasi tabel jika belum ada."""
    conn = None
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_state (
                    id SERIAL PRIMARY KEY,
                    state_key VARCHAR(255) UNIQUE NOT NULL,
                    state_value JSONB NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_state_key ON bot_state (state_key);
            """)
            conn.commit()
            logger.info("Tabel database 'bot_state' telah diinisialisasi atau sudah ada.")
    except Exception as e:
        logger.error(f"Gagal menginisialisasi database: {e}. Trace: {traceback.format_exc()}")
    finally:
        if conn:
            conn.close()

def save_bot_state_to_db(key, value):
    """Menyimpan state bot ke database."""
    conn = None
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            logger.info(f"Attempting to save state '{key}' to database. Data type: {type(value)}")
            #json_value_str = json.dumps(value)
            json_value = Json(value)
            cur.execute(sql.SQL("""
                INSERT INTO bot_state (state_key, state_value)
                VALUES (%s, %s)
                ON CONFLICT (state_key) DO UPDATE
                SET state_value = EXCLUDED.state_value, last_updated = CURRENT_TIMESTAMP;
            """), (key, json_value))
            conn.commit()
            logger.info(f"State '{key}' berhasil disimpan ke database.")
    except Exception as e:
        logger.error(f"Gagal menyimpan state '{key}' ke database: {e}. Trace: {traceback.format_exc()}")
        if conn:
            conn.rollback() # Rollback jika ada error
    finally:
        if conn:
            conn.close()

def load_bot_state_from_db(key):
    """Memuat state bot dari database."""
    conn = None
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            cur.execute(sql.SQL("SELECT state_value FROM bot_state WHERE state_key = %s;"), (key,))
            result = cur.fetchone()
            if result:
                logger.info(f"State '{key}' berhasil dimuat dari database.")
                return result[0] # result[0] adalah kolom JSONB
            else:
                logger.warning(f"State '{key}' not found in database. Returning None.") # Ubah ke warning
                return None
    except Exception as e:
        logger.error(f"Gagal memuat state '{key}' dari database: {e}. Trace: {traceback.format_exc()}")
        return None
    finally:
        if conn:
            conn.close()
# --- AKHIR FUNGSI BANTU DATABASE ---

def self_ping():
    """Mengirim permintaan HTTP ke URL bot sendiri untuk mencegah idle timeout."""
    if not BOT_PUBLIC_URL:
        logger.warning("BOT_PUBLIC_URL tidak dikonfigurasi. Self-ping dinonaktifkan.")
        return

    try:
        response = requests.get(BOT_PUBLIC_URL, timeout=10)
        if response.status_code == 200:
            logger.debug(f"Self-ping berhasil ke {BOT_PUBLIC_URL}")
        else:
            logger.warning(f"Self-ping ke {BOT_PUBLIC_URL} mengembalikan status {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Gagal melakukan self-ping ke {BOT_PUBLIC_URL}: {e}")
    except Exception as e:
        logger.error(f"Error tak terduga saat self-ping: {e}. Trace: {traceback.format_exc()}")

    # Jadwalkan ping berikutnya
    # Ini penting: membuat ping berulang secara otomatis
    ping_timer = threading.Timer(SELF_PING_INTERVAL_SECONDS, self_ping)
    ping_timer.daemon = True # Memungkinkan program keluar meskipun timer masih berjalan
    ping_timer.start()

def calculate_indicators(candle_data):
    """
    Menghitung indikator teknikal dari data lilin yang sudah terstruktur.
    Memastikan data cukup untuk semua indikator.
    """
    try:
        close_prices = np.array([c['close'] for c in candle_data], dtype=float)
        high_prices = np.array([c['high'] for c in candle_data], dtype=float)
        low_prices = np.array([c['low'] for c in candle_data], dtype=float)

        # Pastikan ada cukup data untuk periode indikator terlama
        required_data_length = max(SMA_FILTER_PERIOD, SMA_SLOW_PERIOD, SMA_FAST_PERIOD, ADX_PERIOD, ATR_PERIOD_STOP_LOSS)# +1 untuk menghindari IndexErrors pada akses [-1]

        if len(close_prices) < required_data_length:
            logger.debug(f"Not enough candle data for indicators. Required: {required_data_length}, Got: {len(close_prices)}")
            return None

        # Hitung indikator. Periksa None dari tulipy jika ada edge case
        sma_fast_val = ti.sma(close_prices, period=SMA_FAST_PERIOD)
        sma_slow_val = ti.sma(close_prices, period=SMA_SLOW_PERIOD)
        sma_filter_val = ti.sma(close_prices, period=SMA_FILTER_PERIOD)
        atr_val = ti.atr(high_prices, low_prices, close_prices, period=ATR_PERIOD_STOP_LOSS)
        adx_val = ti.adx(high_prices, low_prices, close_prices, period=ADX_PERIOD)

        if (sma_fast_val is None or len(sma_fast_val) == 0 or
            sma_slow_val is None or len(sma_slow_val) == 0 or
            sma_filter_val is None or len(sma_filter_val) == 0 or
            atr_val is None or len(atr_val) == 0 or
            adx_val is None or len(adx_val) == 0):
             logger.warning(f"Tulipy returned None/empty array for some indicators with {len(close_prices)} candles.")
             return None

        return {
            "sma_cepat": sma_fast_val[-1],
            "sma_lambat": sma_slow_val[-1],
            "sma_filter": sma_filter_val[-1],
            "atr": atr_val[-1],
            "adx": adx_val[-1]
        }
    except Exception as e:
        logger.error(f"Error calculating indicators: {e}. Trace: {traceback.format_exc()}")
        return None

# --- [3] KELAS BOT UTAMA ---

# --- [Baru] KELAS KLIEN WEBSOCKET INDODAX ---
class IndodaxWebSocketClient:
    def __init__(self, message_queue: Queue, pairs_to_subscribe: list): # Hapus websocket_token dari sini
        self.websocket_url = "wss://ws3.indodax.com/ws/"
        self.message_queue = message_queue
        self.pairs_to_subscribe = pairs_to_subscribe 
        self.ws = None
        self.thread = None
        self.running = False
        self.reconnect_interval = 10 
        self.keep_alive_interval = 30 
        self.last_pong_time = time.time()
        self.last_ping_time = time.time()
        self.stop_event = Event()
        self.authenticated_and_subscribed = False
        logger.info(f"IndodaxWebSocketClient initialized for pairs: {pairs_to_subscribe}")
        
    def _on_open(self, ws):
        logger.info("WebSocket connection opened. Subscribing to kline channels...")
        
        # Reset flag saat koneksi dibuka
        self.authenticated_and_subscribed = True # Langsung set ke True karena tidak ada autentikasi terpisah
        
        # HAPUS BAGIAN PENGIRIMAN AUTHENTIKASI INI:
        # auth_message = {
        #     "params": {
        #         "token": self.websocket_token
        #     },
        #     "id": 1
        # }
        # ws.send(json.dumps(auth_message))
        # logger.info("Authentication message sent. Waiting for response...")
        
        # Sekarang kirim semua pesan subscribe
        for pair in self.subscribed_pairs:
    # Perubahan di sini: Hapus .replace('_', '')
            channel_name = f"kline_{pair.lower()}_{interval}"
            self.ws.send(json.dumps({"method": f"subscribe_{channel_name}", "params":[], "id": 1}))
            self.logger.info(f"Subscribed to {channel_name}")
        
        # Mulai ping loop
        self.last_pong_time = time.time()
        self.last_ping_time = time.time()
        self._send_ping_loop()

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            
            if 'channel' in data and data['channel'].startswith('kline_'):
                # Pastikan sudah terautentikasi dan berlangganan sebelum memproses kline
                # Meskipun server biasanya tidak akan mengirim jika belum, ini adalah cek keamanan
                if not self.authenticated_and_subscribed:
                    logger.warning(f"Received kline data before full authentication/subscription. Skipping. Message: {message}")
                    return

                # Data kline diterima, masukkan ke antrean
                self.message_queue.put({'type': 'kline', 'data': data})
                # logger.debug(f"Received kline data for {data.get('pair')}/{data.get('interval')}: {data.get('data', {}).get('close')}")
            elif 'pong' in data:
                self.last_pong_time = time.time()
                logger.debug("Received WebSocket pong.") # Now explicitly log this for debugging
            elif 'channel' in data and data['channel'] == 'system' and 'message' in data and 'connected' in data['message']:
                logger.info(f"WebSocket system message: {data['message']}")
            else:
                logger.debug(f"Received other WebSocket message: {message}")
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON from WebSocket message: {message}")
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}. Message: {message}. Trace: {traceback.format_exc()}")

    def _on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")
        self.running = False # Tandai bahwa perlu reconnect

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"WebSocket connection closed. Code: {close_status_code}, Msg: {close_msg}. Attempting to reconnect...")
        self.running = False # Tandai bahwa perlu reconnect
        self.authenticated_and_subscribed = False


    def _send_ping_loop(self):
        if self.stop_event.is_set():
            logger.debug("Ping loop stopping.")
            return

        try:
            if self.ws and self.ws.sock and self.ws.sock.status == websocket.STATUS_NORMAL:
                # Periksa apakah ada pong yang diterima baru-baru ini
                if time.time() - self.last_pong_time > self.reconnect_interval * 2: # Jika tidak ada pong dalam 2x interval reconnect
                    logger.warning("No pong received for a long time, forcing WebSocket reconnect.")
                    self.ws.close() # Tutup koneksi untuk memicu reconnect
                    return

                if time.time() - self.last_ping_time > self.keep_alive_interval:
                    logger.debug("Relying on websocket-client's built-in ping mechanism.")
            else:
                logger.debug("WebSocket not connected or sock not available, skipping ping.")
        except Exception as e:
            logger.error(f"Error in WebSocket ping loop: {e}. Trace: {traceback.format_exc()}")
        
        # Jadwalkan ping berikutnya
        if not self.stop_event.is_set():
            Thread(target=lambda: time.sleep(self.keep_alive_interval / 2) or self._send_ping_loop()).start()


    def _run_websocket(self):
        logger.info("WebSocket run_websocket thread started.") # Add this
        ssl_context = ssl.create_default_context()
        while not self.stop_event.is_set():
            if not self.running: 
                logger.info("Attempting to start new WebSocket connection...") # Change for clarity
                try:
                    self.ws = websocket.WebSocketApp(
                        self.websocket_url,
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=self._on_error,
                        on_close=self._on_close
                    )
                    logger.debug(f"Calling run_forever for URL: {self.websocket_url}") # New debug log
                    self.ws.run_forever(
                        ping_interval=self.keep_alive_interval,
                        ping_timeout=self.keep_alive_interval/2,
                        sslopt={"cert_reqs": ssl.CERT_REQUIRED, "context": ssl_context} # Explicitly use default context
                    )
                    logger.info("run_forever exited normally (connection closed).") # New log
                    self.running = True 
                except Exception as e:
                    logger.error(f"Critical error during WebSocket run_forever: {e}. Retrying in {self.reconnect_interval} seconds. Trace: {traceback.format_exc()}") # More detail
                    self.running = False
                
                if not self.running and not self.stop_event.is_set():
                    time.sleep(self.reconnect_interval)
            else:
                time.sleep(1)
        logger.info("WebSocket run_websocket thread stopping.")

    def start(self):
        if self.thread is None or not self.thread.is_alive():
            self.stop_event.clear()
            self.thread = Thread(target=self._run_websocket, daemon=True)
            self.thread.start()
            logger.info("IndodaxWebSocketClient thread started.")

    def stop(self):
        self.stop_event.set()
        if self.ws:
            self.ws.close()
            logger.info("WebSocket connection closed by stop request.")
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5) # Tunggu thread selesai, maks 5 detik
            if self.thread.is_alive():
                logger.warning("WebSocket thread did not terminate gracefully.")
        logger.info("IndodaxWebSocketClient stopped.")

class IndodaxTradingBot:
    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key
        
        self.message_queue = Queue() # Antrean untuk menerima pesan dari WebSocket

        self.candle_database = {}  # {pair: {'candles': deque(maxlen=...)}}
        self.open_positions = {}   # {pair: {stop_loss, highest_price, initial_capital, total_capital, num_buys, avg_price}}
        
        self.last_report_time = 0.0
        self.last_radar_report_time = 0.0
        self.total_equity_idr = 0.0 # Dilacak untuk perhitungan risiko
        # List pasangan yang akan di-subscribe oleh WebSocket
        # Awalnya mungkin kosong, nanti akan diisi saat bot mulai mendapatkan summaries
        self.websocket_pairs_subscribed = [] 
        self.websocket_client = None # Akan diinisialisasi nanti di `run` setelah mendapatkan daftar pairs
        self.websocket_pairs = []
        self.websocket_pairs = self._fetch_initial_market_summaries() 
        self.logger.info(f"IndodaxWebSocketClient initialized for pairs: {self.websocket_pairs}")
        self.ws_client = IndodaxWebSocketClient(
            pairs=self.websocket_pairs,
            interval=self.kline_interval,
            logger=self.logger
        )
        self.ws_client.start()

        # Muat state bot jika ada
        loaded_state = load_bot_state_from_db('bot_main_state')
        if loaded_state:
            self.candle_database = loaded_state.get('candle_database', {})
            self.open_positions = loaded_state.get('open_positions', {})
            self.last_report_time = loaded_state.get('last_report_time', 0.0)
            self.last_radar_report_time = loaded_state.get('last_radar_report_time', 0.0)
            logger.info("Bot state successfully restored from database.")
        else:
            logger.warning("No previous bot state found. Starting fresh.")
        
        max_candle_length = max(SMA_FILTER_PERIOD, SMA_SLOW_PERIOD, SMA_FAST_PERIOD, ADX_PERIOD, ATR_PERIOD_STOP_LOSS) + 10 # Tambah buffer
        for pair, data in self.candle_database.items():
            if 'candles' in data and isinstance(data['candles'], list):
                self.candle_database[pair]['candles'] = deque(data['candles'], maxlen=max_candle_length)
            else:
                self.candle_database[pair]['candles'] = deque(maxlen=max_candle_length)
                logger.warning(f"[{pair}] 'candles' data not found or invalid on load. Initializing empty deque.")
            if 'current_interval_data' not in data:
                 self.candle_database[pair]['current_interval_data'] = {
                    'timestamp': None, 'open': None, 'high': None, 'low': None, 'prices_in_interval': []
                }
        logger.info("IndodaxTradingBot initialized.")

    def _get_account_info(self):
        """Mengambil informasi akun dari Indodax."""
        try:
            info = indodax_request('getInfo', {}, self.api_key, self.secret_key)
            return info['return'] # Langsung kembalikan bagian 'return' yang relevan
        except IndodaxAPIError:
            logger.error("Failed to get account info.")
            return None
    
    def _process_websocket_messages(self):
        """
        Memproses semua pesan yang ada di antrean dari WebSocket.
        """
        max_candle_length = max(SMA_FILTER_PERIOD, SMA_SLOW_PERIOD, SMA_FAST_PERIOD, ADX_PERIOD, ATR_PERIOD_STOP_LOSS) + 10

        while not self.message_queue.empty():
            try:
                message = self.message_queue.get(block=False)
                logger.debug(f"Processing message from queue: Type={message.get('type')}, Pair={message.get('data', {}).get('pair')}")
                if message['type'] == 'kline':
                    kline_data = message['data']
                    pair = kline_data.get('pair')
                    
                    if not pair:
                        logger.warning(f"Received kline message without 'pair': {kline_data}. Skipping.")
                        continue

                    kline_ohlc = kline_data.get('data')
                    if not kline_ohlc:
                        logger.warning(f"Received kline message for {pair} without 'data': {kline_data}. Skipping.")
                        continue

                    # Klines dari Indodax WebSocket memberikan data close_time.
                    # Kita akan menggunakan timestamp ini sebagai timestamp lilin yang sudah selesai.
                    # Data 'kline' berisi: [open, high, low, close, volume, close_time]
                    timestamp_ms = kline_ohlc[5] # close_time dalam milidetik
                    timestamp_sec = timestamp_ms / 1000

                    # Format lilin agar sesuai dengan struktur yang diharapkan oleh calculate_indicators
                    new_candle = {
                        'timestamp': timestamp_sec,
                        'open': float(kline_ohlc[0]),
                        'high': float(kline_ohlc[1]),
                        'low': float(kline_ohlc[2]),
                        'close': float(kline_ohlc[3]),
                        # 'volume': float(kline_ohlc[4]) # Volume bisa ditambahkan jika ingin digunakan
                    }

                    if pair not in self.candle_database:
                        self.candle_database[pair] = {
                            'candles': deque(maxlen=max_candle_length),
                            # `current_interval_data` tidak lagi diperlukan untuk lilin websocket
                            # Tapi kita bisa menyimpannya sebagai placeholder jika ada logika lain yang menggunakannya
                            'current_interval_data': {
                                'timestamp': None, 'open': None, 'high': None, 'low': None, 'prices_in_interval': []
                            }
                        }
                        logger.info(f"[{pair}] Initializing new candle_database entry.")
                    
                    candles_deque = self.candle_database[pair]['candles']
                    
                    # Indodax WebSocket akan mengirim kline "baru" (yang sedang berjalan) dan "selesai".
                    # Jika timestamp lilin yang diterima adalah timestamp lilin terakhir di deque, itu berarti update
                    # lilin yang sedang berjalan. Jika tidak, itu lilin yang baru.
                    if candles_deque and candles_deque[-1]['timestamp'] == new_candle['timestamp']:
                        # Update lilin terakhir (jika ini adalah update lilin yang sedang berjalan)
                        candles_deque[-1] = new_candle
                        logger.debug(f"[{pair}] Updated running candle. C:{new_candle['close']:.0f}. Deque size: {len(candles_deque)}")
                    else:
                        # Ini adalah lilin baru atau lilin yang baru saja selesai
                        # Jika lilin yang baru saja selesai, pastikan tidak ada duplikasi
                        # WebSocket kadang bisa mengirim lilin selesai + lilin baru secara berurutan
                        # atau lilin selesai dan kemudian lilin yang sama lagi.
                        if not candles_deque or candles_deque[-1]['timestamp'] < new_candle['timestamp']:
                            candles_deque.append(new_candle)
                            logger.info(f"[{pair}] ADDED new candle. O:{new_candle['open']:.0f} H:{new_candle['high']:.0f} L:{new_candle['low']:.0f} C:{new_candle['close']:.0f} at {datetime.fromtimestamp(new_candle['timestamp']).strftime('%H:%M')}. Deque size: {len(candles_deque)}")
                        # else:
                            logger.debug(f"[{pair}] Duplicate or older candle received. Current last: {datetime.fromtimestamp(candles_deque[-1]['timestamp']).strftime('%H:%M')}, New: {datetime.fromtimestamp(new_candle['timestamp']).strftime('%H:%M')}. Skipping.")

                else:
                    logger.warning(f"Unknown message type received from WebSocket: {message['type']}")
                
                self.message_queue.task_done()
            except Queue.Empty:
                # Seharusnya tidak terjadi karena loop while not empty
                break
            except Exception as e:
                logger.error(f"Error processing WebSocket message from queue: {e}. Message: {message}. Trace: {traceback.format_exc()}") 

    def _sync_open_positions(self, account_balances, current_market_data):
        """
        Menyinkronkan posisi terbuka yang dicatat bot dengan saldo aktual di akun.
        Menghapus posisi dari `self.open_positions` jika koin tidak lagi dimiliki.
        Ini penting untuk konsistensi.
        """
        actual_coins_in_account = {coin.lower() + "_idr" for coin, amount_str in account_balances.items() 
                                   if coin.lower() != 'idr' and float(amount_str) > 0.00000001}
        
        # Buat daftar posisi untuk dihapus
        positions_to_remove = [pair for pair in self.open_positions if pair not in actual_coins_in_account]

        for pair in positions_to_remove:
            logger.info(f"[SYNC] Removing {pair.split('_')[0].upper()} from open_positions as it's no longer in account.")
            del self.open_positions[pair]
        
        # Rekonsiliasi: Jika ada koin di akun yang tidak ada di open_positions
        # Ini bisa terjadi jika bot gagal menyimpan state atau ada trade manual.
        for pair in actual_coins_in_account:
            if pair not in self.open_positions:
                logger.warning(f"[SYNC] Found {pair.split('_')[0].upper()} in account but not in bot's open_positions. Adding as a 'manual' position.")
                
                # Coba estimasi SL dan harga_rata_rata
                # Ini adalah tebakan terbaik karena kita tidak tahu history-nya
                current_price = current_market_data.get(pair, {}).get('last', 0.0)
                coin_amount = float(account_balances.get(pair.split('_')[0], 0.0))
                
                if current_price > 0 and coin_amount > 0:
                    # Estimasi modal dan harga rata-rata
                    estimated_total_capital = current_price * coin_amount
                    
                    # Coba set SL ke harga saat ini atau sedikit di bawahnya
                    # Ini adalah solusi darurat, tidak ideal tanpa data pembelian awal
                    estimated_sl = current_price * 0.95 # Misalnya 5% di bawah harga saat ini
                    
                    self.open_positions[pair] = {
                        'stop_loss': estimated_sl,
                        'highest_price': current_price,
                        'modal_awal': estimated_total_capital, # Ini bisa tidak akurat
                        'total_modal': estimated_total_capital,
                        'jumlah_pembelian': 1, # Asumsikan satu kali pembelian
                        'harga_rata_rata': current_price # Asumsikan harga rata-rata = harga saat ini
                    }
                    kirim_notifikasi_telegram(f"⚠️ *POSISI REKONSILIASI*\n\n*{pair.split('_')[0].upper()}/IDR* terdeteksi di akun tetapi tidak tercatat bot. Ditambahkan sebagai posisi baru dengan SL estimasi `Rp {estimated_sl:,.0f}`.")

    def _manage_open_positions(self, market_summaries, account_balances, calculated_indicators):
        """Mengelola posisi yang sudah terbuka: trailing stop-loss, sinyal jual, pyramiding."""
        saldo_idr_saat_ini = float(account_balances.get('idr', 0.0))
        logger.debug(f"[_manage_open_positions] Managing {len(self.open_positions)} open positions.")
        
        for pair, position_detail in list(self.open_positions.items()): # Iterasi pada salinan list untuk memungkinkan modifikasi
            coin_id = pair.split('_')[0]
            current_price = market_summaries.get(pair, {}).get('last', 0.0)

            if current_price == 0:
                logger.warning(f"Market data for {pair} not found or price is zero. Skipping position management.")
                continue

            # Perbarui lilin untuk pair ini dan hitung indikator
            # Pastikan ada data lilin yang cukup sebelum menghitung indikator
            if pair not in self.candle_database or len(self.candle_database[pair]['candles']) < max(SMA_FILTER_PERIOD, SMA_SLOW_PERIOD, SMA_FAST_PERIOD, ADX_PERIOD, ATR_PERIOD_STOP_LOSS):
                logger.debug(f"Not enough candle data for {pair} to calculate indicators in _manage_open_positions. Skipping.")
                continue

            #indicators = calculate_indicators(list(self.candle_database[pair]['candles']))
            indicators = calculated_indicators.get(pair) # Ambil yang sudah dihitung
            if not indicators: # Cek jika indikator gagal dihitung
                logger.warning(f"Indicators not found for {pair} in _manage_open_positions. Skipping.")
                continue

            # Trailing Stop-Loss
            if current_price > position_detail['highest_price']:
                atr_value = indicators['atr']
                new_stop_loss = current_price - (ATR_FACTOR_STOP_LOSS * atr_value)
                if new_stop_loss > position_detail['stop_loss']: # Hanya naikkan SL
                    self.open_positions[pair]['stop_loss'] = new_stop_loss
                    self.open_positions[pair]['highest_price'] = current_price
                    logger.info(f"[TL] Stop-loss for {coin_id.upper()} raised to Rp {new_stop_loss:,.0f}")
            
            stop_loss_triggered = current_price < position_detail['stop_loss']
            sell_signal_active = indicators['sma_cepat'] < indicators['sma_lambat']

            # Kondisi Penjualan
            if stop_loss_triggered or sell_signal_active:
                reason = "Trailing Stop Loss" if stop_loss_triggered else "Death Cross Signal"
                logger.info(f"[SELL SIGNAL - {reason}] for {coin_id.upper()}. Current price: {current_price:,.0f}, SL: {position_detail['stop_loss']:,.0f}")
                
                coin_amount_to_sell = float(account_balances.get(coin_id, 0.0))
                if coin_amount_to_sell > 0:
                    try:
                        params_sell = {"pair": pair, "type": "sell", coin_id: coin_amount_to_sell}
                        response_sell = indodax_request('trade', params_sell, self.api_key, self.secret_key)
                        
                        if response_sell and response_sell.get('success') == 1:
                            profit_loss_percent = ((current_price - position_detail['harga_rata_rata']) / position_detail['harga_rata_rata']) * 100 if position_detail['harga_rata_rata'] > 0 else 0
                            
                            sell_message = (
                                f"📉 *POSISI DITUTUP*\n\nAlasan: *{reason}*\n*{coin_id.upper()}/IDR* dijual.\n"
                                f"Harga beli rata-rata: `Rp {position_detail['harga_rata_rata']:,.0f}`\n"
                                f"Harga jual: `Rp {current_price:,.0f}`\n"
                                f"P/L: `{profit_loss_percent:.2f}%`"
                            )
                            kirim_notifikasi_telegram(sell_message)
                            del self.open_positions[pair]
                            logger.info(f"Successfully sold {coin_id.upper()}.")
                            time.sleep(5) # Pause setelah transaksi
                    except IndodaxAPIError as e:
                        logger.error(f"Failed to execute sell order for {coin_id.upper()}: {e}")
                else:
                    logger.warning(f"No {coin_id.upper()} amount to sell for pair {pair}.")
                    del self.open_positions[pair] # Hapus dari posisi terbuka jika saldo sudah 0

            # Pyramiding
            elif USE_PYRAMIDING and position_detail['jumlah_pembelian'] < MAX_PYRAMID_BUYS_PER_COIN:
                profit_percent = ((current_price - position_detail['harga_rata_rata']) / position_detail['harga_rata_rata']) * 100 if position_detail['harga_rata_rata'] > 0 else 0
                
                if profit_percent >= PYRAMID_TARGET_PROFIT_PERCENT:
                    additional_capital = position_detail['modal_awal'] * PYRAMID_SCALE_FACTOR
                    
                    if saldo_idr_saat_ini >= additional_capital:
                        logger.info(f"[PYRAMIDING] Adding to position for {coin_id.upper()} with Rp {additional_capital:,.0f}...")
                        try:
                            params_buy_pyramid = {"pair": pair, "type": "buy", "idr": int(additional_capital)}
                            response_buy_pyramid = indodax_request('trade', params_buy_pyramid, self.api_key, self.secret_key)
                            
                            if response_buy_pyramid and response_buy_pyramid.get('success') == 1:
                                # Perbarui saldo dan re-calculate avg price
                                updated_account_info = self._get_account_info()
                                if updated_account_info:
                                    updated_balances = updated_account_info['balance']
                                    total_coin_new = float(updated_balances.get(coin_id, 0.0))
                                    total_capital_new = position_detail['total_modal'] + additional_capital

                                    self.open_positions[pair]['jumlah_pembelian'] += 1
                                    self.open_positions[pair]['total_modal'] = total_capital_new
                                    self.open_positions[pair]['harga_rata_rata'] = total_capital_new / total_coin_new if total_coin_new > 0 else 0
                                    
                                    # SL disesuaikan untuk mengunci breakeven dari seluruh posisi
                                    #self.open_positions[pair]['stop_loss'] = self.open_positions[pair]['harga_rata_rata']
                                    new_calculated_sl_pyramid = current_price - (ATR_FACTOR_STOP_LOSS * indicators['atr'])
                                    self.open_positions[pair]['stop_loss'] = max(
                                    self.open_positions[pair]['stop_loss'], # SL lama
                                    self.open_positions[pair]['harga_rata_rata'], # Breakeven
                                    new_calculated_sl_pyramid # SL baru berdasarkan ATR
                                )
                                    
                                    pyramid_message = (
                                        f"📈 *POSISI DITAMBAH (Pyramiding)*\n\n*{coin_id.upper()}/IDR* ditambah `Rp {additional_capital:,.0f}`.\n"
                                        f"Harga rata-rata baru: `Rp {self.open_positions[pair]['harga_rata_rata']:,.0f}`.\n"
                                        f"SL disesuaikan ke breakeven: `Rp {self.open_positions[pair]['stop_loss']:,.0f}`."
                                    )
                                    kirim_notifikasi_telegram(pyramid_message)
                                    logger.info(f"Successfully pyramided {coin_id.upper()}.")
                                    time.sleep(5) # Pause setelah transaksi
                                else:
                                    logger.error(f"Failed to get updated account info after pyramiding {coin_id.upper()}.")
                            else:
                                logger.error(f"Pyramiding order for {coin_id.upper()} failed or returned no success.")
                        except IndodaxAPIError as e:
                            logger.error(f"Failed to execute pyramiding order for {coin_id.upper()}: {e}")
                    else:
                        logger.warning(f"Insufficient IDR for pyramiding {coin_id.upper()}. Needed: {additional_capital:,.0f}, Have: {saldo_idr_saat_ini:,.0f}")

    def _find_new_opportunities(self, market_summaries, account_balances, calculated_indicators):
        """Mencari peluang beli baru berdasarkan strategi."""
        logger.debug(f"[_find_new_opportunities] Searching for new opportunities. Current open positions: {len(self.open_positions)}.")
        if len(self.open_positions) >= MAKSIMAL_KOIN_DITRADING:
            logger.info("Max coin positions reached. Skipping new buy opportunities.")
            return

        saldo_idr_saat_ini = float(account_balances.get('idr', 0.0))
        if saldo_idr_saat_ini < MINIMUM_INVESTMENT_IDR:
            logger.warning(f"Insufficient IDR balance for new trades. Have: {saldo_idr_saat_ini:,.0f}, Minimum: {MINIMUM_INVESTMENT_IDR:,.0f}")
            return

        candidates = []
        for pair, summary in market_summaries.items():
            if pair in self.open_positions:
                continue # Jangan beli koin yang sudah ada di posisi terbuka

            coin_id = pair.split('_')[0]
            
            # --- PENAMBAHAN: Cek Spread Maksimal ---
            buy_price = summary.get('buy', 0.0)
            sell_price = summary.get('sell', 0.0)
            if buy_price <= 0 or sell_price <= 0:
                logger.debug(f"Skipping {pair}: buy/sell prices not available.")
                continue

            spread_percent = ((sell_price - buy_price) / buy_price) * 100
            if spread_percent > MAX_SPREAD_PERCENT:
                logger.debug(f"Skipping {pair} due to wide spread: {spread_percent:.2f}% > {MAX_SPREAD_PERCENT:.2f}%")
                continue
            # --- AKHIR PENAMBAHAN ---
            
            # Pastikan ada data lilin yang cukup untuk indikator
            if pair not in self.candle_database or len(self.candle_database[pair]['candles']) < max(SMA_FILTER_PERIOD, SMA_SLOW_PERIOD, SMA_FAST_PERIOD, ADX_PERIOD, ATR_PERIOD_STOP_LOSS):
                # logger.debug(f"Not enough candle data for {pair} to calculate indicators for new opportunity.")
                continue

            #indicators = calculate_indicators(list(self.candle_database[pair]['candles']))
            indicators = calculated_indicators.get(pair) # Ambil yang sudah dihitung
            if not indicators:
                logger.debug(f"Indicators not found for {pair} in _find_new_opportunities. Skipping.")
                continue

            # Trinity Signal using robust internal candle data
            current_close_price = summary['last'] # Gunakan harga 'last' dari ringkasan
            
            # Tren utama bullish (harga di atas SMA filter)
            main_trend_bullish = current_close_price > indicators['sma_filter']
            # Tren lokal kuat (ADX di atas threshold)
            local_trend_strong = indicators['adx'] > MINIMUM_ADX_VALUE
            # Sinyal beli aktif (golden cross SMA cepat di atas SMA lambat)
            buy_signal_active = indicators['sma_cepat'] > indicators['sma_lambat']

            if main_trend_bullish and local_trend_strong and buy_signal_active:
                candidates.append({
                    'pair': pair,
                    'coin_id': coin_id,
                    'atr': indicators['atr'],
                    'current_price': current_close_price,
                    'adx_value': indicators['adx'], # Untuk ranking
                    'buy_price_for_limit': buy_price # Tambahkan harga bid untuk limit order
                })
        
        # Urutkan kandidat berdasarkan kekuatan ADX (tren lokal) dari yang tertinggi
        candidates.sort(key=lambda x: x['adx_value'], reverse=True)

        if candidates:
            best_candidate = candidates[0]
            logger.info(f"Best candidate found: {best_candidate['coin_id'].upper()} with ADX {best_candidate['adx_value']:.2f}")

            risk_per_trade_idr = self.total_equity_idr * (TOTAL_RISK_PER_EQUITY_PERCENT / 100)
            atr_idr = best_candidate['atr']

            stop_loss_distance = ATR_FACTOR_STOP_LOSS * atr_idr
            
            if stop_loss_distance <= 0: # Pastikan SL distance valid
                logger.warning(f"Stop loss distance is zero or negative for {best_candidate['coin_id'].upper()}. Skipping.")
                return

            # Hitung jumlah koin yang akan dibeli berdasarkan risiko yang diizinkan
            # Formula: (Risk Capital / SL Distance) = Jumlah Koin
            # Modal untuk beli = Jumlah Koin * Harga Saat Ini
            coin_amount_to_buy_based_on_risk = risk_per_trade_idr / stop_loss_distance
            capital_to_invest_raw = coin_amount_to_buy_based_on_risk * best_candidate['current_price']
                        # ... (lanjutan dari _find_new_opportunities) ...

            # Pastikan modal yang diinvestasikan memenuhi minimum, jika tidak, batalkan pembelian
            if capital_to_invest_raw < MINIMUM_INVESTMENT_IDR:
                logger.warning(f"Calculated capital for {best_candidate['coin_id'].upper()} ({capital_to_invest_raw:,.0f} IDR) is below MINIMUM_INVESTMENT_IDR ({MINIMUM_INVESTMENT_IDR:,.0f} IDR). Skipping.")
                return # Jangan buka posisi jika modal terlalu kecil

            modal_untuk_beli = int(capital_to_invest_raw) # Bulatkan ke integer untuk API Indodax

            if saldo_idr_saat_ini >= modal_untuk_beli:
                logger.info(f"[ACTION] Buying {best_candidate['coin_id'].upper()}. Dynamic position size: Rp {modal_untuk_beli:,.0f}")
                try:
                    params_buy = {
                        "pair": best_candidate['pair'], 
                        "type": "buy", 
                        "idr": modal_untuk_beli, 
                        "price": best_candidate['buy_price_for_limit'] # Harga limit order
                    }
                    response_buy = indodax_request('trade', params_buy, self.api_key, self.secret_key)
                    
                    if response_buy and response_buy.get('success') == 1:
                        # Indodax API untuk 'trade' dengan limit order akan langsung mengembalikan
                        # status order. Kita perlu memeriksa apakah order langsung terisi atau pending.
                        order_id = response_buy.get('order_id')
                        # Untuk menyederhanakan, kita asumsikan jika success=1, order berhasil dipasang.
                        # Tapi kita perlu VERIFIKASI apakah order TERISI.
                        
                        # --- Perbaikan: Polling Order Status (opsional, untuk bot yang lebih kompleks) ---
                        # Untuk bot ini, kita akan langsung cek saldo setelahnya. Jika ada saldo, berarti terisi.
                        # Ini mungkin kurang robust jika order partial fill, tapi untuk Indodax biasanya langsung.
                        # Idealnya, kita akan memantau order_id sampai status 'filled' atau 'cancelled'.
                        
                        time.sleep(5) # Beri waktu agar transaksi diproses dan saldo terupdate
                        
                        # Setelah pembelian berhasil, segera update info akun untuk mendapatkan saldo koin yang dibeli
                        updated_account_info = self._get_account_info()
                        if updated_account_info:
                            updated_balances = updated_account_info['balance']
                            bought_coin_amount = float(updated_balances.get(best_candidate['coin_id'], 0.0))
                            
                            # JIKA JUMLAH KOIN YANG DIBELI LEBIH DARI 0, BARU LANJUTKAN PROSES INI
                            # Indentasi IF ini harus sejajar dengan baris di atasnya (bought_coin_amount = ...)
                            if bought_coin_amount > 0: 
                                actual_avg_price = modal_untuk_beli / bought_coin_amount if bought_coin_amount > 0 else best_candidate['current_price']
                                
                                # STOP LOSS AWAL: Lebih baik dihitung dari HARGA RATA-RATA beli yang sudah terkonfirmasi
                                # atau dari low lilin terakhir jika ingin lebih agresif/spesifik pada swing point.
                                # Untuk menjaga kesederhanaan, kita akan hitung dari harga rata-rata beli.
                                initial_stop_loss = actual_avg_price - stop_loss_distance
                                
                                self.open_positions[best_candidate['pair']] = {
                                    'stop_loss': initial_stop_loss,
                                    'highest_price': best_candidate['current_price'], # Highest price dimulai dari harga saat ini
                                    'modal_awal': modal_untuk_beli,
                                    'total_modal': modal_untuk_beli,
                                    'jumlah_pembelian': 1,
                                    'harga_rata_rata': actual_avg_price 
                                }
                                buy_message = (
                                    f"📈 *POSISI BARU DIBUKA*\n\n*{best_candidate['coin_id'].upper()}/IDR* dibeli `Rp {modal_untuk_beli:,.0f}`.\n"
                                    f"Harga rata-rata: `Rp {actual_avg_price:,.0f}`\n"
                                    f"SL awal: `Rp {initial_stop_loss:,.0f}`."
                                )
                                kirim_notifikasi_telegram(buy_message)
                                logger.info(f"Successfully bought {best_candidate['coin_id'].upper()}.")
                                time.sleep(5) # Pause setelah transaksi
                            else: # ELSE ini pasangan dari IF bought_coin_amount > 0
                                logger.error(f"Limit buy order for {best_candidate['coin_id'].upper()} placed, but no coin amount detected in balance. Order ID: {order_id}. It might be pending or failed to fill.")
                                #logger.error(f"Buy order for {best_candidate['coin_id'].upper()} succeeded, but no coin amount detected in balance (amount was {bought_coin_amount}).")
                                kirim_notifikasi_telegram(f"⚠️ *ORDER BELI PENDING/GAGAL*\n\nLimit order beli *{best_candidate['coin_id'].upper()}/IDR* (`Rp {modal_untuk_beli:,.0f}`) tidak terisi. Cek manual di Indodax. Order ID: `{order_id}`.")
                        else: # ELSE ini pasangan dari IF updated_account_info
                            logger.error(f"Failed to get updated account info after buying {best_candidate['coin_id'].upper()}.")
                    else: # ELSE ini pasangan dari IF response_buy and response_buy.get('success') == 1
                        logger.error(f"Buy order for {best_candidate['coin_id'].upper()} failed or returned no success (response: {response_buy}).")
                except IndodaxAPIError as e:
                    logger.error(f"Failed to execute buy order for {best_candidate['coin_id'].upper()}: {e}")
            else: # ELSE ini pasangan dari IF saldo_idr_saat_ini >= modal_untuk_beli
                logger.warning(f"Insufficient IDR balance for {best_candidate['coin_id'].upper()}. Needed: {modal_untuk_beli:,.0f}, Have: {saldo_idr_saat_ini:,.0f}")
        else: # ELSE ini pasangan dari IF candidates:
            logger.info("No suitable buy candidates found.")

    def _send_portfolio_report(self, account_balances, market_summaries):
        """Mengirim laporan portofolio ke Telegram."""
        current_time = time.time()
        if current_time - self.last_report_time < REPORT_INTERVAL_SECONDS:
            return

        logger.info("Generating Portfolio Report...")
        saldo_idr = float(account_balances.get('idr', 0.0))
        total_coin_asset_value = 0
        asset_message = ""

        for coin_id, amount_str in account_balances.items():
            amount = float(amount_str)
            if coin_id.lower() != 'idr' and amount > 0.00000001:
                pair = f"{coin_id.lower()}_idr"
                last_price = market_summaries.get(pair, {}).get('last', 0.0)
                if last_price > 0:
                    idr_value = amount * last_price
                    total_coin_asset_value += idr_value
                    asset_message += f"▪️ *{coin_id.upper()}:* `{amount:.6f}` (~Rp {idr_value:,.0f})\n"
        
        total_equity = saldo_idr + total_coin_asset_value
        title = "🗓️ *Laporan Portofolio Harian*" if self.last_report_time != 0 else "🚀 *Bot Telah Aktif! Laporan Awal:*"
        
        report_message = (
            f"{title}\n\nTotal Estimasi Ekuitas:\n`Rp {total_equity:,.0f}`\n\n"
            f"💵 *Saldo IDR:*\n`Rp {saldo_idr:,.0f}`\n\n"
        )
        if asset_message:
            report_message += f"💎 *Aset Koin Saat Ini:*\n{asset_message}"
        else:
            report_message += "💎 *Aset Koin Saat Ini:*\n_(Tidak ada posisi terbuka)_\n"
        
        kirim_notifikasi_telegram(report_message)
        self.last_report_time = current_time
        logger.info("Portfolio Report sent.")

    def _send_market_radar_report(self, calculated_indicators):
        """Mengirim laporan radar pasar ke Telegram."""
        current_time = time.time()
        if current_time - self.last_radar_report_time < RADAR_REPORT_INTERVAL_HOURS * 3600:
            return

        logger.info("Generating Market Radar Report...")
        bullish_candidates = []
        bearish_candidates = []

        for pair, indicators in calculated_indicators.items():
            last_candle_close = self.candle_database[pair]['candles'][-1]['close'] if self.candle_database[pair]['candles'] else 0.0

            if last_candle_close > indicators['sma_filter']:
                bullish_candidates.append(pair.split('_')[0])
            else:
                bearish_candidates.append(pair.split('_')[0])
            
            # Gunakan harga penutupan lilin terakhir yang dihitung bot
            #last_candle_close = data_entry['candles'][-1]['close'] 
        
        radar_message = "📡 *Laporan Radar Pasar (Internal)*\n\n"
        if bullish_candidates:
            radar_message += f"🔥 *Koin Bullish ({len(bullish_candidates)}):*\n`" + ', '.join(k.upper() for k in bullish_candidates[:10]) + "`\n"
        if bearish_candidates:
            radar_message += f"\n❄️ *Koin Bearish ({len(bearish_candidates)}):*\n`" + ', '.join(k.upper() for k in bearish_candidates[:10]) + "`\n"
        if self.open_positions:
            radar_message += "\n🛡️ *Posisi Aktif:*\n`" + ', '.join(p.split('_')[0].upper() for p in self.open_positions.keys()) + "`"
        
        kirim_notifikasi_telegram(radar_message)
        self.last_radar_report_time = current_time
        logger.info("Market Radar Report sent.")
        
    def run(self):
        """Metode utama untuk menjalankan loop bot."""
        logger.info(f"--- [INDODAX BOT] Memulai bot v35 (Penyempurnaan Demigod + WebSocket)... ---")
        
        # Periksa kredensial sebelum memulai loop utama
        if not all([self.api_key, self.secret_key]):
            logger.critical("API Key atau Secret Key Indodax tidak ditemukan. Bot tidak dapat berjalan.")
            kirim_notifikasi_telegram("⛔ *BOT GAGAL STARTUP*\n\nAPI Key atau Secret Key Indodax tidak ditemukan.")
            return

        # Uji koneksi API awal
        try:
            self._get_account_info()
            logger.info("Indodax API connection test successful.")
        except IndodaxAPIError:
            logger.critical("Gagal terhubung ke Indodax API. Periksa kredensial dan koneksi internet.")
            kirim_notifikasi_telegram("⛔ *BOT GAGAL STARTUP*\n\nGagal terhubung ke Indodax API. Periksa kredensial atau IP whitelisting.")
            return
        except Exception as e:
            logger.critical(f"Unexpected error during initial API test: {e}. Trace: {traceback.format_exc()}")
            kirim_notifikasi_telegram(f"⛔ *BOT GAGAL STARTUP*\n\nTerjadi error tak terduga saat tes API awal: {str(e)[:500]}.")
            return
        
        # --- [BARU] Inisialisasi WebSocket Client ---
        logger.info("Fetching initial market summaries to determine WebSocket pairs...")
        initial_market_summaries = get_indodax_public_summaries()
        if not initial_market_summaries:
            logger.critical("Failed to get initial market summaries. Cannot determine pairs for WebSocket. Bot cannot start.")
            kirim_notifikasi_telegram("⛔ *BOT GAGAL STARTUP*\n\nGagal mendapatkan ringkasan pasar awal untuk inisialisasi WebSocket.")
            return
        
        # Ambil semua pair yang qualified dari initial_market_summaries
        self.websocket_pairs_subscribed = list(initial_market_summaries.keys())
        self.websocket_pairs_subscribed = list(set(self.websocket_pairs_subscribed))
        
        # Inisialisasi dan mulai WebSocket client
        if self.websocket_client is None:
            # PENTING: JANGAN PASS self.websocket_token DI SINI
            self.websocket_client = IndodaxWebSocketClient(self.message_queue, self.websocket_pairs_subscribed) 
            self.websocket_client.start()
            logger.info(f"IndodaxWebSocketClient started for {len(self.websocket_pairs_subscribed)} pairs.")
        # --- AKHIR INISIALISASI WEBSOCKET ---

        # --- Tambahan: Kirim Laporan Awal Saat Startup Pertama ---
        try:
            initial_account_info = self._get_account_info()
            if initial_market_summaries and initial_account_info:
                initial_account_balances = initial_account_info['balance']
                
                initial_total_equity_idr = float(initial_account_balances.get('idr', 0.0))
                for pair, position_detail in self.open_positions.items():
                    current_price = initial_market_summaries.get(pair, {}).get('last', 0.0)
                    coin_id = pair.split('_')[0]
                    amount_held = float(initial_account_balances.get(coin_id, 0.0))
                    if current_price > 0 and amount_held > 0:
                        initial_total_equity_idr += amount_held * current_price
                self.total_equity_idr = initial_total_equity_idr
                
                self.last_report_time = 0.0
                self._send_portfolio_report(initial_account_balances, initial_market_summaries)
                self.last_report_time = time.time()
                logger.info("Laporan awal berhasil dikirim ke Telegram.")
            else:
                logger.warning("Tidak dapat mengirim laporan awal: Gagal mendapatkan ringkasan pasar atau info akun.")
        except Exception as e:
            logger.error(f"Error saat mengirim laporan awal: {e}. Trace: {traceback.format_exc()}")
        # --- Akhir Tambahan ---
    
        warmup_period_length = max(SMA_FILTER_PERIOD, SMA_SLOW_PERIOD, SMA_FAST_PERIOD, ADX_PERIOD, ATR_PERIOD_STOP_LOSS) + 10 # Harus sama

        # Durasi tidur antar siklus loop utama. Ini bisa lebih pendek sekarang karena WebSocket
        # mengalirkan data secara terus-menerus. Kita bisa cek setiap 10-30 detik.
        main_loop_sleep_duration = 30 # Detik

        while True:
            try:
                logger.info(f"\n[{datetime.now().strftime('%H:%M:%S')}] --- MEMULAI SIKLUS PENGUMPULAN DATA & ANALISIS ---")

                # --- [BARU] Proses semua pesan dari WebSocket ---
                self._process_websocket_messages()
                # --- AKHIR PROSES WEBSOCKET ---

                # Perbarui summaries secara berkala (misal setiap 5 menit) untuk harga 'last' dan 'buy'/'sell'
                # Ini masih diperlukan untuk _sync_open_positions, _find_new_opportunities (spread check), dan pelaporan
                # Tapi jangan memanggilnya terlalu sering untuk menghindari rate limit.
                if time.time() % (5 * 60) < main_loop_sleep_duration: # Panggil setiap 5 menit
                    market_summaries = get_indodax_public_summaries()
                    if not market_summaries:
                        logger.warning("Failed to get market summaries. Using old summaries for price checks.")
                        # Jika gagal, mungkin gunakan summaries terakhir yang berhasil diambil atau data harga terakhir dari lilin.
                        # Untuk menyederhanakan, kita biarkan saja jika gagal dan log warning.
                # Jika tidak dalam siklus 5 menit, gunakan summaries terakhir yang berhasil diambil.
                # Anda perlu menyimpan market_summaries di self jika ingin ini.
                # Untuk saat ini, kita akan tetap memanggilnya di awal siklus jika ingin selalu fresh.
                # ATAU: Kita bisa memperbarui market_summaries hanya untuk data buy/sell/last dari WebSocket jika Indodax menyediakan.
                # Untuk saat ini, kita tetap memanggil REST API summaries secara berkala.
                # Mari kita tambahkan mekanisme untuk menyimpan summaries terakhir.

                # Menambahkan mekanisme untuk menyimpan summaries terakhir
                if not hasattr(self, '_last_market_summaries') or time.time() - self._last_market_summaries_time > (5 * 60):
                    new_market_summaries = get_indodax_public_summaries()
                    if new_market_summaries:
                        self._last_market_summaries = new_market_summaries
                        self._last_market_summaries_time = time.time()
                        market_summaries = self._last_market_summaries
                    else:
                        logger.warning("Failed to get fresh market summaries. Using previous ones.")
                        market_summaries = getattr(self, '_last_market_summaries', {})
                else:
                    market_summaries = self._last_market_summaries
                
                if not market_summaries: # Jika bahkan _last_market_summaries juga kosong
                    logger.warning("No market summaries available. Skipping trading logic in this cycle.")
                    time.sleep(main_loop_sleep_duration)
                    continue

                account_info = self._get_account_info()
                if not account_info:
                    logger.warning("Failed to get account info. Waiting for next cycle.")
                    time.sleep(main_loop_sleep_duration)
                    continue
                
                account_balances = account_info['balance']
                
                # --- [0.5] Pembaruan Database Lilin (DIHAPUS, DIGANTI OLEH WEBSOCKET) ---
                # self._update_candle_database(market_summaries) # Baris ini DIHAPUS

                # Cek apakah ada cukup data lilin di *setidaknya satu* pair yang qualified
                warmup_period_length = max(SMA_FILTER_PERIOD, SMA_SLOW_PERIOD, SMA_FAST_PERIOD, ADX_PERIOD, ATR_PERIOD_STOP_LOSS) + 10
                
                # Filter pairs yang sudah memiliki cukup lilin untuk dihitung indikator
                qualified_pairs_for_indicators = {}
                for pair, data_entry in self.candle_database.items():
                    # Pastikan pair yang ada di candle_database juga ada di market_summaries
                    # dan memiliki cukup lilin.
                    if pair in market_summaries and len(data_entry['candles']) >= warmup_period_length:
                        qualified_pairs_for_indicators[pair] = data_entry

                if not qualified_pairs_for_indicators:
                    # Logika ini perlu diperbaiki karena lilin datang dari WS sekarang.
                    # Jika tidak ada lilin sama sekali, berarti WS belum memberikan data
                    # atau tidak ada pair yang memenuhi `warmup_period_length`
                    for pair_debug, data_entry_debug in self.candle_database.items():
                        if 'candles' in data_entry_debug:
                            logger.debug(f"DEBUG: {pair_debug} has {len(data_entry_debug['candles'])} candles.")
                        else:
                            logger.debug(f"DEBUG: {pair_debug} has no 'candles' key.")
                    
                    total_candles_processed = sum(len(d['candles']) for d in self.candle_database.values())
                    logger.info(f"[INFO] Sedang dalam fase pemanasan WebSocket. Total lilin terkumpul: {total_candles_processed}. Membutuhkan {warmup_period_length} lilin per pair untuk indikator.")
                    time.sleep(main_loop_sleep_duration)
                    continue

                # --- [0.6] Hitung Indikator Sekali untuk Semua Pair yang Qualified ---
                calculated_indicators = {}
                for pair, data_entry in qualified_pairs_for_indicators.items():
                    indicators = calculate_indicators(list(data_entry['candles']))
                    if indicators:
                        calculated_indicators[pair] = indicators
                    else:
                        logger.warning(f"Failed to calculate indicators for {pair}.")
                
                if not calculated_indicators: # Jika tidak ada indikator yang berhasil dihitung
                    logger.warning("No indicators calculated for any qualified pair. Skipping trading logic.")
                    time.sleep(main_loop_sleep_duration)
                    continue


                # --- [1] Rekonsiliasi & Perhitungan Ekuitas ---
                self._sync_open_positions(account_balances, market_summaries)

                self.total_equity_idr = float(account_balances.get('idr', 0.0))
                for pair, position_detail in self.open_positions.items():
                    current_price = market_summaries.get(pair, {}).get('last', 0.0) # Gunakan last price dari summaries
                    coin_id = pair.split('_')[0]
                    amount_held = float(account_balances.get(coin_id, 0.0))
                    if current_price > 0 and amount_held > 0:
                        self.total_equity_idr += amount_held * current_price
                
                logger.info(f"Total Ekuitas: Rp {self.total_equity_idr:,.0f} | Saldo IDR: Rp {float(account_balances.get('idr', 0.0)):,.0f} | Posisi Aktif: {len(self.open_positions)}/{MAKSIMAL_KOIN_DITRADING}")

                # --- [2] Pelaporan Periodik ---
                self._send_portfolio_report(account_balances, market_summaries)
                self._send_market_radar_report(calculated_indicators)

                # --- [3] Manajemen Posisi Terbuka ---
                self._manage_open_positions(market_summaries, account_balances, calculated_indicators)

                # --- [4] Pencarian Peluang Baru ---
                self._find_new_opportunities(market_summaries, account_balances, calculated_indicators)
                
                logger.info("Attempting to save bot state to database...")

                # --- [5] Simpan State Bot ---
                state_to_save = {
                    'candle_database': {p: {
                        'candles': list(d['candles']),
                        # 'current_interval_data' tidak lagi aktif digunakan untuk agregasi.
                        # Kita bisa menghapusnya atau menyimpannya sebagai dict kosong jika diperlukan untuk kompatibilitas.
                        # Untuk kejelasan, mari kita simpan sebagai dict kosong jika tidak ada,
                        # agar strukturnya tetap konsisten jika ada kode lama yang masih merujuk.
                        'current_interval_data': d.get('current_interval_data', {}) 
                    } for p, d in self.candle_database.items()},
                    'open_positions': self.open_positions,
                    'last_report_time': self.last_report_time,
                    'last_radar_report_time': self.last_radar_report_time,
                }
                
                try:
                    json.dumps(state_to_save)
                    logger.debug("State data is JSON-serializable.")
                except TypeError as e:
                    logger.error(f"State data is NOT JSON-serializable! Error: {e}. Full state_to_save: {state_to_save}")
                    for pair, data in state_to_save['candle_database'].items():
                        if not isinstance(data.get('candles'), list):
                             logger.error(f"Candles for {pair} is not a list!")
                        if not isinstance(data.get('current_interval_data'), dict):
                             logger.error(f"Current interval data for {pair} is not a dict!")
                        high_val = data.get('current_interval_data', {}).get('high')
                        low_val = data.get('current_interval_data', {}).get('low')
                        if high_val is not None and (math.isinf(high_val) or math.isnan(high_val)):
                            logger.error(f"High price for {pair} is invalid: {high_val}")
                        if low_val is not None and (math.isinf(low_val) or math.isnan(low_val)):
                            logger.error(f"Low price for {pair} is invalid: {low_val}")
                    pass
                
                logger.debug(f"Calling save_bot_state_to_db with key 'bot_main_state'. Candle count for btc_idr: {len(self.candle_database.get('btc_idr', {}).get('candles', []))}")
                
                save_bot_state_to_db('bot_main_state', state_to_save)
                
                logger.info("Bot state save attempt completed.")

                logger.info(f"--- Siklus Selesai. Menunggu {main_loop_sleep_duration} detik... ---\n")
                time.sleep(main_loop_sleep_duration) # Tidur sebentar agar tidak terlalu sering loop
                
            except KeyboardInterrupt:
                logger.info("Bot stopped by user (KeyboardInterrupt).")
                kirim_notifikasi_telegram("🛑 *BOT DIHENTIKAN*\n\nBot dihentikan secara manual.")
                if self.websocket_client:
                    self.websocket_client.stop() # Hentikan WebSocket client
                try:
                    if 'state_to_save' in locals():
                        save_bot_state_to_db('bot_main_state', state_to_save)
                        logger.info("Final bot state saved successfully after KeyboardInterrupt.")
                    else:
                        logger.warning("state_to_save not defined when KeyboardInterrupt occurred. Cannot save final state.")
                except Exception as e:
                    logger.error(f"Failed to save final bot state after KeyboardInterrupt: {e}")
                break
            except Exception as e:
                logger.critical(f"FATAL ERROR IN MAIN BOT LOOP: {e}. Trace: {traceback.format_exc()}")
                kirim_notifikasi_telegram(f"🚨 *FATAL ERROR BOT*\n\nTerjadi kesalahan fatal. Bot mungkin berhenti. Detail: ```{str(e)[:500]}...```")
                if self.websocket_client:
                    self.websocket_client.stop() # Hentikan WebSocket client
                try:
                    if 'state_to_save' in locals():
                        save_bot_state_to_db('bot_main_state', state_to_save)
                        logger.info("Bot state saved after fatal error.")
                    else:
                        logger.warning("state_to_save not defined when fatal error occurred. Cannot save state.")
                except Exception as save_e:
                    logger.error(f"Failed to save bot state after fatal error: {save_e}")
                time.sleep(main_loop_sleep_duration * 2) # Tunggu lebih lama setelah error kritis

# --- [4 & 5] Bagian Flask & Main ---

@app.route('/')
def home():
    return "Indodax Trading Bot (Demigod Edition) is active."

logger.info("--- [MODUL] File bot_apex.py sedang dimuat... ---")
if not all([INDODAX_API_KEY, INDODAX_SECRET_KEY]):
    logger.critical("[FATAL ERROR] Kredensial Indodax tidak ditemukan! Bot tidak akan dimulai.")
    kirim_notifikasi_telegram("⛔ *BOT GAGAL STARTUP*\n\nKredensial Indodax (API Key/Secret) tidak ditemukan.")
else:
    logger.info("--- [MODUL] Kredensial ditemukan. Memulai thread bot di latar belakang... ---")
    
    # --- Panggil inisialisasi database di startup ---
    initialize_database()
    # --- Akhir ---
    
    # Inisialisasi bot di luar thread agar bisa diakses oleh fungsi lainnya jika perlu
    # Namun, untuk bot yang sepenuhnya independen, ini cukup
    indodax_bot_instance = IndodaxTradingBot(INDODAX_API_KEY, INDODAX_SECRET_KEY)
    
    bot_thread = threading.Thread(target=indodax_bot_instance.run, daemon=True)
    bot_thread.start()

    # --- Mulai proses self-ping setelah bot utama berjalan ---
    if BOT_PUBLIC_URL:
        logger.info(f"Memulai self-ping ke {BOT_PUBLIC_URL} setiap {SELF_PING_INTERVAL_SECONDS / 60:.0f} menit.")
        ping_timer = threading.Timer(SELF_PING_INTERVAL_SECONDS, self_ping)
        ping_timer.daemon = True
        ping_timer.start()
    # --- Akhir self-ping ---
    

if __name__ == "__main__":
    logger.info("--- [LOKAL] Menjalankan server pengembangan Flask untuk tes... ---")
    port = int(os.environ.get('PORT', 5000))
    
    app.run(host='0.0.0.0', port=port, use_reloader=False)
