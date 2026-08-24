"""Class to handle the import workflow."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime
import logging
import re
from typing import Any

import httpx

from clients.firefly import FireflyClient
from clients.truelayer import TrueLayerClient
from config import Config
from exceptions import TrueLayer2FireflyConnectionError

_LOGGER = logging.getLogger(__name__)

class Import2Firefly:
    """Class to handle the import workflow."""

    def __init__(self) -> None:
        """Initialize the Import class."""
        self._config: Config = Config()
        self._truelayer_client: TrueLayerClient = TrueLayerClient()
        self._firefly_client: FireflyClient = FireflyClient()

        self.start_time = datetime.now()
        self.end_time = None

    async def start_import(self) -> AsyncGenerator[Any, Any]:
        """Start the import process."""

        yield "TrueLayer: Fetching accounts and cards from TrueLayer"
        try:
            truelayer_sources = await self._truelayer_client.get_accounts_and_cards()
        except TrueLayer2FireflyConnectionError as err:
            yield f"Error fetching accounts/cards from TrueLayer: {err}"
            return

        await asyncio.sleep(0)

        if not truelayer_sources:
            yield "No accounts or cards found in TrueLayer"
            return

        for source in truelayer_sources:
            source_kind = source["kind"]
            if source_kind == "card":
                source_label = source.get("display_name", source["account_id"])
            else:
                source_label = source["account_number"].get("iban") or source["account_id"]
            yield f"TrueLayer {source_kind}: {source['account_id']} - {source_label}"
            await asyncio.sleep(0)

        yield f"TrueLayer: A total of {len(truelayer_sources)} source(s) found"
        await asyncio.sleep(0)

        yield "Firefly: Fetching accounts from Firefly"
        firefly_accounts = await self._firefly_client.get_account_paginated()
        yield f"Firefly: A total of {len(firefly_accounts)} account(s) found"

        yield "Matching source(s) between TrueLayer and Firefly"

        for truelayer_source in truelayer_sources:
            import_account: dict[str, Any] = {}
            source_kind = truelayer_source["kind"]

            if source_kind == "card":
                tr_label = truelayer_source["account_id"]
                tr_iban = None
            else:
                tr_iban = truelayer_source["account_number"].get("iban")
                tr_label = tr_iban

            yield f"Checking matches for TrueLayer {source_kind} (ID/IBAN): {tr_label}"

            for firefly_account in firefly_accounts:
                if source_kind == "card":
                    ff_number = firefly_account["attributes"].get("account_number")
                    matched = bool(tr_label and tr_label == ff_number)
                else:
                    ff_iban = firefly_account["attributes"].get("iban")
                    matched = tr_iban == ff_iban
                
                if matched:
                    yield f"Matching account found: {tr_label}"
                    
                    attrs = firefly_account.get("attributes", {})
                    account_role = attrs.get("account_role")
                    account_type = attrs.get("type")

                    is_valid_asset = account_role in ["defaultAsset", "ccAsset"]
                    is_valid_liability = account_type == "liability"

                    if is_valid_asset or is_valid_liability:
                        import_account = firefly_account
                        yield f"Firefly account verified successfully (Role: {account_role}, Type: {account_type}), let's continue"
                        break
                    else:
                        yield f"Firefly account matched, but role '{account_role}' or type '{account_type}' is unsupported"
            else:
                if not import_account:
                    yield f"No matching Firefly account found for {tr_label}"            
                    continue

            yield f"TrueLayer: Fetching transactions for {tr_label}..."
            if source_kind == "card":
                raw_response = await self._truelayer_client.get_card_transactions(
                    truelayer_source["account_id"]
                )
            else:
                raw_response = await self._truelayer_client.get_transactions(
                    truelayer_source["account_id"]
                )

            # Optimised TrueLayer Response Direct JSON Extraction Path
            if isinstance(raw_response, httpx.Response):
                try:
                    parsed_res = raw_response.json()
                    txns = parsed_res.get("results", []) if isinstance(parsed_res, dict) else parsed_res
                except Exception:
                    txns = []
            elif isinstance(raw_response, list):
                txns = raw_response
            elif isinstance(raw_response, dict):
                txns = raw_response.get("results", raw_response.get("data", []))
            else:
                txns = []

            if not txns:
                yield "No transactions found in TrueLayer"
                continue
            yield f"TrueLayer: A total of {len(txns)} transaction(s) found"
            yield "TrueLayer: Matching transactions to Firefly account"

            matching = 0
            unmatching = 0
            newly_created = 0
            total_transactions = len(txns)

            try:
                recent_ff_txns = await self._firefly_client._request(
                    uri=f"accounts/{import_account['id']}/transactions?limit=50",
                    method="GET"
                )
                if isinstance(recent_ff_txns, dict) and "data" in recent_ff_txns:
                    recent_data = recent_ff_txns["data"]
                elif isinstance(recent_ff_txns, list):
                    recent_data = recent_ff_txns
                else:
                    recent_data = []
            except Exception as e:
                _LOGGER.info(f"Warning: Could not pre-fetch recent transactions for duplicate check: {e}")
                recent_data = []
            
            for i, txn in enumerate(txns, start=1):
                cp_iban = txn.get("meta", {}).get("counter_party_iban")
                cp_name = txn.get("meta", {}).get("counter_party_preferred_name")
                transaction_type = (
                    "debit" if txn["transaction_type"].lower() == "debit" else "credit"
                )
                
                linked_account = None
                
                is_duplicate = False
                txn_id_str = str(txn["transaction_id"])
                amount = abs(txn["amount"])
                txn_date_short = txn["timestamp"][:10]

                for ff_tx in recent_data:
                    attributes = ff_tx.get("attributes", {})
                    journal_ext_id = str(attributes.get("external_id", ""))
                    inner_transactions = attributes.get("transactions", [])
                    
                    if txn_id_str in journal_ext_id or journal_ext_id in txn_id_str:
                        is_duplicate = True
                        break
                    
                    for inner_t in inner_transactions:
                        inner_ext_id = str(inner_t.get("external_id", ""))
                        inner_linked_id = str(inner_t.get("linked_account_id", ""))
                        
                        if txn_id_str in inner_ext_id or txn_id_str in inner_linked_id:
                            is_duplicate = True
                            break
                        
                        if (
                            float(inner_t.get("amount", 0)) == amount 
                            and inner_t.get("date", "")[:10] == txn_date_short
                            and inner_t.get("description", "").lower() == txn["description"].lower()
                        ):
                            is_duplicate = True
                            break
                    if is_duplicate:
                        break

                if is_duplicate:
                    msg = f"Transaction already exists (Skipped via Pre-Check): {txn['description']} - {txn['amount']} - {txn['timestamp']}"
                    _LOGGER.info(msg)
                    yield msg
                    continue

                if cp_iban is not None or cp_name is not None:
                    for firefly_account in firefly_accounts:
                        if (
                            transaction_type == "debit"
                            and firefly_account["attributes"]["type"] != "expense"
                        ):
                            continue
                        if (
                            transaction_type == "credit"
                            and firefly_account["attributes"]["type"] != "revenue"
                        ):
                            continue

                        if cp_iban and cp_iban == firefly_account["attributes"].get("iban"):
                            yield f"Matching account found via IBAN: {txn['description']} - {cp_iban}"
                            linked_account = firefly_account
                            matching += 1
                            break

                        if cp_name and cp_name == firefly_account["attributes"].get("name"):
                            yield f"Matching account found via name: {txn['description']} - {cp_name}"
                            linked_account = firefly_account
                            matching += 1
                            break

                    if linked_account is None and cp_iban is not None:
                        account_type = "revenue" if transaction_type == "credit" else "expense"
                        yield f"No match, creating fallback virtual account mapping: {txn.get('description')} -> Type: {account_type}"
                else:
                    unmatching += 1

                amount = abs(txn["amount"])

                if amount == 0:
                    description_text = txn.get("description", "")
                    amount_match = re.search(r'\b\d+\.\d+\b', description_text)
                    
                    if amount_match:
                        amount = float(amount_match.group(0))
                        _LOGGER.info(f"[FOREIGN CURRENCY TEXT MATCH] Parsed cost '{amount}' directly out of description narrative: '{description_text}'")
                    else:
                        _LOGGER.info(f"Skipping zero-value transaction (Authorization check): {description_text}")
                        continue

                DEFAULT_CHECKING_ACCOUNT_ID = "1"
                DEFAULT_CHECKING_ACCOUNT_NAME = "Main Checking Account"
                for ff_acc in firefly_accounts:
                    if ff_acc.get("attributes", {}).get("account_role") == "defaultAsset":
                        DEFAULT_CHECKING_ACCOUNT_ID = ff_acc["id"]
                        DEFAULT_CHECKING_ACCOUNT_NAME = ff_acc["attributes"]["name"]
                        break

                tl_cat = str(txn.get("transaction_category", "UNKNOWN")).upper()
                tl_class_list = txn.get("transaction_classification", [])

                if isinstance(tl_class_list, list) and tl_class_list:
                    tl_class = str(tl_class_list)
                elif tl_class_list:
                    tl_class = str(tl_class_list)
                else:
                    tl_class = tl_cat.replace("_", " ").title()

                is_negative_amount = txn["amount"] < 0

                is_statement_payment = (
                    source_kind == "card"
                    and is_negative_amount
                    and linked_account is None
                )

                is_pending = txn.get("status", "").lower() == "pending" or "pending" in txn.get("uri", "").lower()
                tx_tags = []
                if is_pending:
                    tx_tags.append("truelayer-pending")

                if is_statement_payment:
                    tx_direction_type = "transfer"
                else:
                    tx_direction_type = "deposit" if transaction_type == "credit" else "withdrawal"

                tx_payload = {
                    "description": txn["description"],
                    "date": txn["timestamp"],
                    "amount": amount,
                    "category_name": tl_class if not is_statement_payment else "",
                    "account_id": import_account["id"],
                    "linked_account_id": txn["transaction_id"],
                    "external_id": f"{txn['transaction_id']}_{tx_direction_type}",
                    "tags": tx_tags,
                }

                if is_statement_payment:
                    tx_payload.update({
                        "type": "transfer",
                        "source_id": DEFAULT_CHECKING_ACCOUNT_ID,
                        "source_name": DEFAULT_CHECKING_ACCOUNT_NAME,
                        "destination_id": import_account["id"],
                        "destination_name": import_account["attributes"]["name"],
                    })
                    _LOGGER.info(f"Generic Intercept (Transfer): Routing statement payment dynamically from '{DEFAULT_CHECKING_ACCOUNT_NAME}' to {import_account['attributes']['name']}")
                elif source_kind == "card" and is_negative_amount:
                    tx_payload.update({
                        "type": "deposit",
                        "destination_id": import_account["id"],
                        "destination_name": import_account["attributes"]["name"],
                        "source_id": linked_account["id"],
                        "source_name": linked_account["attributes"]["name"],
                    })
                    _LOGGER.info(f"Generic Intercept (Refund Deposit): Mapping money return from '{linked_account['attributes']['name']}' to {import_account['attributes']['name']}")
                else:
                    tx_payload.update({
                        "type": "deposit" if transaction_type == "credit" else "withdrawal",
                        "destination_id": (
                            import_account["id"]
                            if transaction_type == "credit"
                            else (None if linked_account is None else linked_account["id"])
                        ),
                        "destination_name": (
                            import_account["attributes"]["name"]
                            if transaction_type == "credit"
                            else ("(unknown expense account)" if linked_account is None else linked_account["attributes"]["name"])
                        ),
                        "source_id": (
                            (None if linked_account is None else linked_account["id"])
                            if transaction_type == "credit"
                            else import_account["id"]
                        ),
                        "source_name": (
                            ("(unknown revenue account)" if linked_account is None else linked_account["attributes"]["name"])
                            if transaction_type == "credit"
                            else import_account["attributes"]["name"]
                        ),
                    })

                import_transaction = {
                    "error_if_duplicate_hash": True,
                    "apply_rules": True,
                    "fire_webhooks": True,
                    "transactions": [tx_payload],
                }

                response = None
                try:
                    response = await self._firefly_client.create_transaction(
                        import_transaction
                    )
                except TrueLayer2FireflyConnectionError as e:
                    error_msg = str(e)
                    if "422" in error_msg and "Duplicate of transaction" in error_msg:
                        is_duplicate = True
                    else:
                        _LOGGER.info(f"Error creating transaction in Firefly: {error_msg}")
                        yield f"Error creating transaction in Firefly: {error_msg}"
                        continue
                except Exception as e:
                    _LOGGER.info(f"Unexpected error creating transaction in Firefly: {e}")
                    yield f"Unexpected error creating transaction in Firefly: {e}"
                    continue

                tx_type = tx_payload.get("type")
                if tx_type == "transfer":
                    source_display = tx_payload.get("source_name")
                    dest_display = tx_payload.get("destination_name")
                elif tx_type == "deposit":
                    source_display = tx_payload.get("source_name") or "(unknown revenue account)"
                    dest_display = tx_payload.get("destination_name")
                else:
                    source_display = tx_payload.get("source_name")
                    dest_display = tx_payload.get("destination_name") or "(unknown expense account)"

                if is_duplicate:
                    msg = f"Transaction already exists (Skipped): {txn['description']} - {amount} - {txn['timestamp']}"
                    _LOGGER.info(msg)
                    yield msg
                elif response is not None:
                    newly_created += 1
                    if tx_type == "transfer":
                        msg = f"Transfer created: {txn['description']} | Date: {txn['timestamp']} | Amount: £{amount} | From: {source_display} -> To: {dest_display}"
                    else:
                        msg = f"Transaction created: {txn['description']} | Date: {txn['timestamp']} | Amount: £{amount} | From: {source_display} -> To: {dest_display} ({tx_type.title()})"
                    _LOGGER.info(msg)
                    yield msg
                else:
                    msg = f"Transaction processed: {txn['description']} - {amount} - {txn['timestamp']}"
                    _LOGGER.info(msg)
                    yield msg

                await asyncio.sleep(0)

                yield {
                    "type": "progress",
                    "data": {
                        "account": tr_label,
                        "current": i,
                        "total": total_transactions,
                    },
                }
                await asyncio.sleep(0.05)

            yield f"Report for {tr_label}: {matching} matching links, {unmatching} unmatched, {newly_created} records created"
            await asyncio.sleep(0)
