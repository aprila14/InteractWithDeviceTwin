import logging
import azure.functions as func
from azure.iot.hub import IoTHubRegistryManager
import os
import json
import traceback

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Function triggered: updateExternalControl")

    try:
        # Get and validate connection string
        conn_str = os.getenv("IOTHUB_CONNECTION_STRING")
        if not conn_str:
            logging.error("❌ IOTHUB_CONNECTION_STRING is not set.")
            return func.HttpResponse("Internal error: Missing IoT Hub connection string", status_code=500)
        
        logging.info("✅ IOTHUB_CONNECTION_STRING loaded successfully.")

        # Parse input
        body = req.get_json()
        device_id = body.get("deviceId")
        desired_props = body.get("desired")

        if not device_id or not isinstance(desired_props, dict):
            return func.HttpResponse(
                "Request must include 'deviceId' (string) and 'desired' (object).",
                status_code=400
            )

        # Connect to IoT Hub
        registry_manager = IoTHubRegistryManager(conn_str)
        logging.info(f"🔗 Connected to IoT Hub. Target device: {device_id}")

        # Build patch
        patch = {
            "properties": {
                "desired": desired_props
            }
        }

        logging.info(f"📦 Patch to apply: {json.dumps(patch)}")

        # Apply patch
        twin = registry_manager.get_twin(device_id)
        registry_manager.update_twin(device_id, patch, twin.etag)

        logging.info(f"✅ Device twin updated for '{device_id}' with properties: {list(desired_props.keys())}")
        return func.HttpResponse(
            f"Successfully updated twin for device '{device_id}' with properties: {list(desired_props.keys())}",
            status_code=200
        )

    except Exception as e:
        logging.error("❌ Exception occurred:", exc_info=True)
        return func.HttpResponse(f"Internal server error: {str(e)}", status_code=500)
