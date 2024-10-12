import json
from constants import *
from alert_manager import AlertManager

def create_response(status_code: int, **kwargs):
    response = {
                'statusCode': status_code,
                'body': {

                },
            }

    for k, v in kwargs.items():
        response["body"][str(k)] = json.dumps(v)

    return response


def lambda_handler(event, context):
    alert_manager = AlertManager("laba3ed-alerts")

    body = json.loads(event.get("body","{}"))

    # Extract the action from the event
    action = body.get("action", None)

    # Handle registration
    if action == ACTION_REGISTER:
        alert = body.get("alert", None)
        if alert is None:
            return create_response(400, message = "Alert details are missing")

        try:
            uid = alert_manager.register_alert(
                alert.get("cities", []),
                alert.get("categories", []),
                alert.get("phone_number", ""),
                alert.get("name", "")
            )
            return create_response(201, message = "Alert registered successfully", alert_uid = uid)
        except Exception as e:
            return create_response(400, message = f"Failed to register alert: {str(e)}") 

    # Handle deregistration
    elif action == ACTION_DEREGISTER:
        alert_uid = body.get("alert_uid", None)
        if alert_uid is None:
            return create_response(400, message = "alert_uid is required for deregistration")

        try:
            alert_manager.deregister_alert(alert_uid)
            return create_response(200, message = f"Alert {alert_uid} deregistered successfully")
        except Exception as e:
            return create_response(400, message = f"Failed to deregister alert {str(alert_uid)}: {str(e)}")

    # Handle matching request
    elif action == ACTION_MATCH:
        request = body.get("request", None)
        if request is None:
            return create_response(400, message = "request data is required for matching")
        city = request.get("city", None)
        category = request.get("category", None)
        phone_number = request.get("phone_number", None)
        description = request.get("description", "")

        # Validate required parameters
        if not city or not category or not phone_number:
            return create_response(400, message = "city, category, and phone_number are required for matching")
        try:
            # Call match_request to find matching alerts
            matching_alerts = alert_manager.match_request(city, category, phone_number, description)
            return create_response(200, message = f"Found {len(matching_alerts)} matching alerts", alerts = matching_alerts) 
        except Exception as e:
            return create_response(400, message = f"Failed to match request: {str(e)}")

    # Invalid action handling
    else:
        return create_response(400, message = f"Invalid action: '{action}'")
