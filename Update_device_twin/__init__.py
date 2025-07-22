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

        # Validate that desired properties contain expected keys
        expected_keys = {
            "DemoMode", "DeviceModel", "ExternalControl", "FlowState", 
            "PressureState", "VoCuState", "currentCalibration", "flowMeterCalibration"
        }
        
        # Log the properties being updated
        logging.info(f"🔍 Updating device twin with properties: {list(desired_props.keys())}")
        
        # Optional: Validate that all provided keys are expected (uncomment if strict validation needed)
        # unexpected_keys = set(desired_props.keys()) - expected_keys
        # if unexpected_keys:
        #     logging.warning(f"⚠️ Unexpected properties found: {unexpected_keys}")
        
        # Type validation and conversion for specific properties
        if "DemoMode" in desired_props:
            # Ensure DemoMode is a boolean
            if not isinstance(desired_props["DemoMode"], bool):
                if isinstance(desired_props["DemoMode"], str):
                    desired_props["DemoMode"] = desired_props["DemoMode"].lower() in ['true', '1', 'yes', 'on']
                else:
                    desired_props["DemoMode"] = bool(desired_props["DemoMode"])
        
        if "DeviceModel" in desired_props:
            # Ensure DeviceModel is an integer
            if not isinstance(desired_props["DeviceModel"], int):
                try:
                    desired_props["DeviceModel"] = int(desired_props["DeviceModel"])
                except ValueError:
                    return func.HttpResponse(
                        "DeviceModel must be a valid integer.",
                        status_code=400
                    )
        
        if "ExternalControl" in desired_props:
            # Ensure ExternalControl is an integer
            if not isinstance(desired_props["ExternalControl"], int):
                try:
                    desired_props["ExternalControl"] = int(desired_props["ExternalControl"])
                except ValueError:
                    return func.HttpResponse(
                        "ExternalControl must be a valid integer.",
                        status_code=400
                    )
        
        if "FlowState" in desired_props:
            # Ensure FlowState is a boolean
            if not isinstance(desired_props["FlowState"], bool):
                if isinstance(desired_props["FlowState"], str):
                    desired_props["FlowState"] = desired_props["FlowState"].lower() in ['true', '1', 'yes', 'on']
                else:
                    desired_props["FlowState"] = bool(desired_props["FlowState"])
        
        if "PressureState" in desired_props:
            # Ensure PressureState is a boolean
            if not isinstance(desired_props["PressureState"], bool):
                if isinstance(desired_props["PressureState"], str):
                    desired_props["PressureState"] = desired_props["PressureState"].lower() in ['true', '1', 'yes', 'on']
                else:
                    desired_props["PressureState"] = bool(desired_props["PressureState"])
        
        if "VoCuState" in desired_props:
            # Ensure VoCuState is a boolean
            if not isinstance(desired_props["VoCuState"], bool):
                if isinstance(desired_props["VoCuState"], str):
                    desired_props["VoCuState"] = desired_props["VoCuState"].lower() in ['true', '1', 'yes', 'on']
                else:
                    desired_props["VoCuState"] = bool(desired_props["VoCuState"])
        
        if "currentCalibration" in desired_props:
            # Ensure currentCalibration is a float
            if not isinstance(desired_props["currentCalibration"], (int, float)):
                try:
                    desired_props["currentCalibration"] = float(desired_props["currentCalibration"])
                except ValueError:
                    return func.HttpResponse(
                        "currentCalibration must be a valid number.",
                        status_code=400
                    )
        
        if "flowMeterCalibration" in desired_props:
            # Ensure flowMeterCalibration is a float
            if not isinstance(desired_props["flowMeterCalibration"], (int, float)):
                try:
                    desired_props["flowMeterCalibration"] = float(desired_props["flowMeterCalibration"])
                except ValueError:
                    return func.HttpResponse(
                        "flowMeterCalibration must be a valid number.",
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

        logging.info(f"📦 Patch to apply: {json.dumps(patch, indent=2)}")

        # Apply patch
        twin = registry_manager.get_twin(device_id)
        registry_manager.update_twin(device_id, patch, twin.etag)

        # Create a summary of updated properties with their values
        updated_summary = {k: v for k, v in desired_props.items()}
        
        logging.info(f"✅ Device twin updated for '{device_id}' with properties: {json.dumps(updated_summary, indent=2)}")
        return func.HttpResponse(
            f"Successfully updated twin for device '{device_id}' with properties: {json.dumps(updated_summary)}",
            status_code=200
        )

    except Exception as e:
        logging.error("❌ Exception occurred:", exc_info=True)
        return func.HttpResponse(f"Internal server error: {str(e)}", status_code=500)
