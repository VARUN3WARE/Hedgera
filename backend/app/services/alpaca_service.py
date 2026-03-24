import httpx

ALPACA_PAPER_BASE_URL = "https://paper-api.alpaca.markets"

async def verify_alpaca_keys(api_key: str, secret_key: str):
    """
    Calls Alpaca /v2/account to validate API credentials.
    Returns (True, account_data) if valid else (False, error_message).
    """
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{ALPACA_PAPER_BASE_URL}/v2/account",
                headers={
                    "APCA-API-KEY-ID": api_key,
                    "APCA-API-SECRET-KEY": secret_key,
                }
            )

        if r.status_code != 200:
            return False, "Invalid Alpaca API Key or Secret Key"

        return True, r.json()

    except Exception as e:
        return False, str(e)
