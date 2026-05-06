import datetime as dt
import logging
import pendulum

from airflow import DAG, models
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

_logger: logging.Logger = logging.getLogger("generacion_insumos_todos_cloud")
_timezone: pendulum.Timezone = pendulum.timezone("America/Bogota")
_email_regex_pattern = r"^[a-zA-Z0-9._%+-]+@(telefonica\.com|sqasa\.co|stefanini\.com|latam\.stefanini\.com)$"

_id_type_values_display = {
    "CC": "Cédula de Ciudadania",
    "CE": "Cédula de extranjeria",
    "PS": "Pasaporte",
    "31": "NIT",
    "18": "Permiso Permanencia Temporal",
}

_business_operation_values_display = {
    "cambio_numero": "Cambio de número",
    "cambio_sim_card": "Cambio de SIM card",
    "serializacion_cambio_sim_card": "Serialización Cambio de SIM card",
    "venta_recurso": "Venta de recurso",
    "pos_2_pre": "Pos 2 Pre",
    "recarga": "Recarga",
    "desactivacion": "Desactivación",
    "venta_nueva_prepago": "Venta nueva prepago",
    "venta_nueva_pospago": "Venta nueva pospago",
}

_change_reason_values_display = {
    "dano": "Daño",
    "cambio_tecnologia": "Cambio Tecnología",
    "perdida_robo": "Perdida/Robo",
    "solicitud_cliente": "Solicitud del cliente",
}

_sell_reason_values_display = {
    "0": "Campaña - Migración",
    "1": "Campañas - Plan empleado",
    "2": "Campañas - Ventas en tienda",
    "3": "Corrección de recursos",
    "4": "Otros",
    "5": "Recursos de campaña",
    "6": "Reno/Repo",
    "7": "Seguro por daños",
    "8": "Seguro por robo",
    "9": "Venta a cuotas",
    "10": "Venta de recursos por página WEB",
}

_effective_method_values_display = {
    "inmediatamente": "Inmediatamente",
    "proximo_ciclo_facturacion": "Próximo ciclo de facturación",
}

_pos2pre_change_reason_values_display = {
    "queja_cliente": "Queja del cliente",
    "retencion": "Retencion",
    "solicitud_cliente": "Solicitud del cliente",
}

_cancellation_reason_values_display = {
    "mejor_oferta_precio": "Oferta - Mejor oferta precio",
    "precio_terminal": "Oferta - Precio terminal/venta cuotas",
    "mejor_oferta_producto": "Oferta - Mejor oferta producto",
    "renovacion_beneficio": "Oferta - Renovación beneficio",
    "cobro_reconexion": "Facturación - Cobro reconexión",
    "pago_no_aplicado": "Facturación - Pago no aplicado",
    "no_recibe_factura": "Facturación - No recibe factura",
    "cobro_no_reconocido": "Facturación - Cobro no reconocido",
    "inconforme_proceso_venta": "Posventa - Inconforme proceso venta",
    "incumplimiento_promesa": "Posventa - Incumplimiento promesa",
    "inconforme_servicio_tecnico": "Posventa - Inconforme servicio técnico",
    "radicacion_tercero_ce": "Cliente - Radicación tercero ce",
    "fallecimiento": "Cliente - Fallecimiento",
    "reduccion_costos": "Cliente - Reducción costos",
    "cierre_negocio": "Cliente - Cierre negocio/fin contrato",
    "calidad_funcionamiento": "Red - Calidad y funcionamiento",
    "sin_cobertura": "Red - Sin cobertura",
}

_deactivation_expiration_time_values_display = {
    "expira_siguiente_ciclo": "Expira el siguiente ciclo de facturación",
    "inmediatamente": "Inmediatamente",
    "vigente_desde": "Vigente desde",
}

_sim_type_prepaid_values_display = {
    "multi_simcard_pre_lte": "MULTI SIMCARD PRE LTE 128K 2FF/3FF/4FF",
    "esim_card_thales_digitalpre_orq": "eSIM CARD THALES DIGITALPRE ORQ",
    "esim_card_thales_digitalpre_orq": "eSIM CARD THALES DIGITALPRE ORQ",
    "esim_card_thales_digitalpos_qr_generico": "eSIM CARD THALES DIGITALPOS QR GENERICO",
    "esim_card_digital_qr_pre": "eSIM CARD DIGITAL QR PRE",
    "esim_card_digital_qr_pos": "eSIM CARD DIGITAL QR POS",
    "esim_card_evoucherpre_qr_piloto": "eSIM CARD EVOUCHERPRE QR PILOTO",
    "multi_simcard_pre_lte": "MULTI SIMCARD PRE LTE 128K 2FF/3F/4F ENR",
    "sim_card_gemal_pre_lte": "SIM CARD GEMAL PRE LTE 128K 2/3/4FF HALF",
    "sim_card_valid_pre_lte": "SIM CARD VALID PRE LTE 128K 2/3/4FF HALF",
    "sim_pre_gd": "SIM PRE G&D 256K-LTER8-2/3/4FF-PRE",
    "sim_pre_idemia": "SIM PRE IDEMIA 256K-LTER8-2/3/4FF-PRE",
    "sim_pre_idem_lte": "SIM PRE IDEM LTE 128K 2/3/4FF HALF SIM",
    "sim_pre_thales": "SIM PRE THALES 256K-LTER8-2/3/4FF-PRE",
    "sim_pre_valid": "SIM PRE VALID 256K-LTER8-2/3/4FF-PRE",
}

_sim_type_pospaid_values_display = {
    "multi_simcard_pospa_lte": r"MULTI SIMCARD POSPA LTE 128K 2FF/3FF/4FF",
    "esim_card_digital_qr_pos": r"eSIM CARD DIGITAL QR POS",
    "esim_card_evoucherpos_qr_digital": r"eSIM CARD EVOUCHERPOS QR DIGITAL",
    "esim_card_evoucherpre_qr_piloto": r"eSIM CARD EVOUCHERPRE QR PILOTO",
    "sim_card_gemal_pos_lte": r"SIM CARD GEMAL POS LTE 128K 2/3/4FF HALF",
    "sim_card_valid_pos_lte": r"SIM CARD VALID POS LTE 128K 2/3/4FF HALF",
    "sim_pos_gd": r"SIM POS G&D 256K-LTER8-2/3/4FF",
    "sim_pos_idemia": r"SIM POS IDEMIA 256K-LTER8-2/3/4FF",
    "sim_pos_idem_lte": r"SIM POS IDEM LTE 128K 2/3/4FF HALF SIM",
    "sim_pos_thales": r"SIM POS THALES 256K-LTER8-2/3/4FF",
    "sim_pos_valid": r"SIM POS VALID 256K-LTER8-2/3/4FF",
}

_device_batch_values_display = {
    "nuevo": "NUEVO",
    "triangulado": "Triangulado",
    "pruebas": "Pruebas",
    "tercero": "Tercero",
}

_sim_batch_values_display = {
    "nuevo": "NUEVO",
    "pruebas": "Pruebas",
}

def determine_business_operation(**kwargs) -> str:
    params: models.Params = kwargs["params"]
    business_operation: str = params["business_operation"]

    assert isinstance(business_operation, str)

    if business_operation == "recarga":
        return "preparar_parametros_recarga"

    elif business_operation == "cambio_sim_card":
        return "preparar_parametros_cambio_sim_card"

    elif business_operation == "serializacion_cambio_sim_card":
        return "preparar_parametros_serializacion_cambio_sim_card"

    elif business_operation == "cambio_numero":
        return "preparar_parametros_cambio_numero"

    elif business_operation == "venta_recurso":
        return "preparar_parametros_venta_recurso"

    elif business_operation == "pos_2_pre":
        return "preparar_parametros_pos2pre"

    elif business_operation == "desactivacion":
        return "preparar_parametros_desactivacion"

    elif business_operation == "venta_nueva_prepago":
        return "preparar_parametros_venta_nueva_prepago"

    elif business_operation == "venta_nueva_pospago":
        return "preparar_parametros_venta_nueva_pospago"

    return "operacion_de_negocio_invalida"


def prepare_recharge_params(**kwargs) -> None:
    task_instance: models.TaskInstance = kwargs["ti"]
    params: models.Params = kwargs["params"]

    email: str = params["email"]
    amount: int = params["amount"]
    status: str = params["status"]
    payment_type: str = params["payment_type"]
    client_type: str = params["client_type"]

    _logger.info("Parámetros de recarga:")
    for param_name, param in [
        ("email", email),
        ("amount", amount),
        ("status", status),
        ("payment_type", payment_type),
        ("client_type", client_type),
    ]:
        _logger.info(f"{param_name}: {param}")

    assert isinstance(email, str)
    assert isinstance(amount, int)
    assert isinstance(status, str)
    assert isinstance(payment_type, str)
    assert isinstance(client_type, str)

    task_instance.xcom_push(key="email", value=email)
    task_instance.xcom_push(key="amount", value=amount)
    task_instance.xcom_push(key="status", value=status)
    task_instance.xcom_push(key="payment_type", value=payment_type)
    task_instance.xcom_push(key="client_type", value=client_type)


def prepare_simcard_change_params(**kwargs) -> None:
    task_instance: models.TaskInstance = kwargs["ti"]
    params: models.Params = kwargs["params"]

    email: str = params["email"]
    amount: int = params["amount"]
    status: str = params["status"]
    client_type: str = params["client_type"]
    payment_type: str = params["payment_type"]
    change_reason: str = params["change_reason"]
    payment_model: str = params["payment_model"]

    _logger.info("Parámetros de cambio de SIM card:")
    for param_name, param in [
        ("email", email),
        ("amount", amount),
        ("status", status),
        ("client_type", client_type),
        ("payment_type", payment_type),
        ("change_reason", change_reason),
        ("payment_model", payment_model),
    ]:
        _logger.info(f"{param_name}: {param}")

    assert isinstance(email, str)
    assert isinstance(amount, int)
    assert isinstance(status, str)
    assert isinstance(client_type, str)
    assert isinstance(payment_type, str)
    assert isinstance(change_reason, str)
    assert isinstance(payment_model, str)

    task_instance.xcom_push(key="email", value=email)
    task_instance.xcom_push(key="amount", value=amount)
    task_instance.xcom_push(key="status", value=status)
    task_instance.xcom_push(key="client_type", value=client_type)
    task_instance.xcom_push(key="payment_type", value=payment_type)
    task_instance.xcom_push(key="change_reason", value=change_reason)
    task_instance.xcom_push(key="payment_model", value=payment_model)


def prepare_number_change_params(**kwargs) -> None:
    task_instance: models.TaskInstance = kwargs["ti"]
    params: models.Params = kwargs["params"]

    email: str = params["email"]
    amount: int = params["amount"]
    status: str = params["status"]
    client_type: str = params["client_type"]
    payment_type: str = params["payment_type"]
    payment_model: str = params["payment_model"]
    number_pattern: str = params["number_pattern"]

    _logger.info("Parámetros de cambio de número móvil:")
    for param_name, param in [
        ("email", email),
        ("amount", amount),
        ("status", status),
        ("client_type", client_type),
        ("payment_type", payment_type),
        ("payment_model", payment_model),
        ("number_pattern", number_pattern),
    ]:
        _logger.info(f"{param_name}: {param}")

    assert isinstance(email, str)
    assert isinstance(amount, int)
    assert isinstance(status, str)
    assert isinstance(client_type, str)
    assert isinstance(payment_type, str)
    assert isinstance(payment_model, str)
    assert isinstance(number_pattern, str)

    task_instance.xcom_push(key="email", value=email)
    task_instance.xcom_push(key="amount", value=amount)
    task_instance.xcom_push(key="status", value=status)
    task_instance.xcom_push(key="client_type", value=client_type)
    task_instance.xcom_push(key="payment_type", value=payment_type)
    task_instance.xcom_push(key="payment_model", value=payment_model)
    task_instance.xcom_push(key="number_pattern", value=number_pattern)

def prepare_serialization_simcard_change_params(**kwargs) -> None:
    task_instance: models.TaskInstance = kwargs["ti"]
    params: models.Params = kwargs["params"]

    email: str = params["email"]
    amount: int = params["amount"]
    order_id: int = params["order_id"]

    _logger.info("Parámetros de serialización cambio de SIM card:")
    for param_name, param in [
        ("email", email),
        ("amount", amount),
        ("order_id", order_id),
    ]:
        _logger.info(f"{param_name}: {param}")

    assert isinstance(email, str)
    assert isinstance(amount, int)
    assert isinstance(order_id, int)

    task_instance.xcom_push(key="email", value=email)
    task_instance.xcom_push(key="amount", value=amount)
    task_instance.xcom_push(key="order_id", value=order_id)

def prepare_mobile_sell_params(**kwargs) -> None:
    task_instance: models.TaskInstance = kwargs["ti"]
    params: models.Params = kwargs["params"]

    email: str = params["email"]
    amount: int = params["amount"]
    status: str = params["status"]
    payment_type: str = params["payment_type"]
    client_type: str = params["client_type"]
    sell_reason: str = params["sell_reason"]
    offer_name: str = params["offer_name"]
    device_color: str = params["device_color"]
    payment_model: str = params["payment_model"]

    _logger.info("Parámetros de venta de recurso:")
    for param_name, param in [
        ("email", email),
        ("amount", amount),
        ("status", status),
        ("payment_type", payment_type),
        ("client_type", client_type),
        ("sell_reason", sell_reason),
        ("offer_name", offer_name),
        ("device_color", device_color),
        ("payment_model", payment_model),
    ]:
        _logger.info(f"{param_name}: {param}")

    assert offer_name is not None
    assert device_color is not None

    assert isinstance(email, str)
    assert isinstance(amount, int)
    assert isinstance(status, str)
    assert isinstance(payment_type, str)
    assert isinstance(client_type, str)
    assert isinstance(payment_model, str)
    assert isinstance(sell_reason, str)
    assert isinstance(offer_name, str)
    assert isinstance(device_color, str)

    task_instance.xcom_push(key="email", value=email)
    task_instance.xcom_push(key="amount", value=amount)
    task_instance.xcom_push(key="status", value=status)
    task_instance.xcom_push(key="payment_type", value=payment_type)
    task_instance.xcom_push(key="client_type", value=client_type)
    task_instance.xcom_push(key="payment_model", value=payment_model)
    task_instance.xcom_push(key="sell_reason", value=sell_reason)
    task_instance.xcom_push(key="offer_name", value=offer_name)
    task_instance.xcom_push(key="device_color", value=device_color)

def prepare_pos2pre_params(**kwargs) -> None:
    task_instance: models.TaskInstance = kwargs["ti"]
    params: models.Params = kwargs["params"]

    email: str = params["email"]
    amount: int = params["amount"]
    status: str = params["status"]
    client_type: str = params["client_type"]
    client_email: str = params["client_email"]
    effective_method: str = params["effective_method"]
    customer_number: int = params["customer_number"]
    change_reason: str = params["pos2pre_change_reason"]

    _logger.info("Parámetros de pos2pre:")
    for param_name, param in [
        ("email", email),
        ("amount", amount),
        ("status", status),
        ("client_type", client_type),
        ("client_email", client_email),
        ("effective_method", effective_method),
        ("customer_number", customer_number),
        ("change_reason", change_reason),
    ]:
        _logger.info(f"{param_name}: {param}")

    assert client_email is not None
    assert effective_method is not None
    assert customer_number is not None
    assert change_reason is not None

    assert isinstance(email, str)
    assert isinstance(amount, int)
    assert isinstance(status, str)
    assert isinstance(client_type, str)
    assert isinstance(client_email, str)
    assert isinstance(effective_method, str)
    assert isinstance(customer_number, int)
    assert isinstance(change_reason, str)

    task_instance.xcom_push(key="email", value=email)
    task_instance.xcom_push(key="amount", value=amount)
    task_instance.xcom_push(key="status", value=status)
    task_instance.xcom_push(key="client_type", value=client_type)
    task_instance.xcom_push(key="client_email", value=client_email)
    task_instance.xcom_push(key="effective_method", value=effective_method)
    task_instance.xcom_push(key="customer_number", value=customer_number)
    task_instance.xcom_push(key="pos2pre_change_reason", value=change_reason)

def prepare_deactivation_params(**kwargs) -> None:
    task_instance: models.TaskInstance = kwargs["ti"]
    params: models.Params = kwargs["params"]

    email: str = params["email"]
    amount: int = params["amount"]
    status: str = params["status"]
    client_type: str = params["client_type"]
    payment_type: str = params["payment_type"]
    cancellation_reason: str = params["cancellation_reason"]
    deactivation_expiration_time: str = params["deactivation_expiration_time"]

    _logger.info("Parámetros de pos2pre:")
    for param_name, param in [
        ("email", email),
        ("amount", amount),
        ("status", status),
        ("client_type", client_type),
        ("payment_type", payment_type),
        ("cancellation_reason", cancellation_reason),
        ("deactivation_expiration_time", deactivation_expiration_time),
    ]:
        _logger.info(f"{param_name}: {param}")

    assert cancellation_reason is not None
    assert deactivation_expiration_time is not None

    assert isinstance(email, str)
    assert isinstance(amount, int)
    assert isinstance(status, str)
    assert isinstance(client_type, str)
    assert isinstance(payment_type, str)
    assert isinstance(cancellation_reason, str)
    assert isinstance(deactivation_expiration_time, str)

    task_instance.xcom_push(key="email", value=email)
    task_instance.xcom_push(key="amount", value=amount)
    task_instance.xcom_push(key="status", value=status)
    task_instance.xcom_push(key="client_type", value=client_type)
    task_instance.xcom_push(key="payment_type", value=payment_type)
    task_instance.xcom_push(key="cancellation_reason", value=cancellation_reason)
    task_instance.xcom_push(key="deactivation_expiration_time", value=deactivation_expiration_time)

def prepare_new_sell_prepaid_params(**kwargs) -> None:
    task_instance: models.TaskInstance = kwargs["ti"]
    params: models.Params = kwargs["params"]

    email: str = params["email"]
    amount: int = params["amount"]
    status: str = params["status"]
    id_type: str = params["id_type"]
    payment_model: str = params["payment_model"]
    number_pattern: str = params["number_pattern"]
    offer_id: str = params["offer_id"]
    imei: str = params["imei"]
    sim_type: str = params["sim_type_prepaid"]
    device_batch: str = params["device_batch"]
    sim_batch: str = params["sim_batch"]
    client_email: str = params["client_email"]
    customer_number: int = params["customer_number"]

    _logger.info("Parámetros de nueva venta prepago:")
    for param_name, param in [
        ("email", email),
        ("amount", amount),
        ("status", status),
        ("id_type", id_type),
        ("payment_model", payment_model),
        ("number_pattern", number_pattern),
        ("offer_id", offer_id),
        ("imei", imei),
        ("sim_type", sim_type),
        ("device_batch", device_batch),
        ("sim_batch", sim_batch),
        ("client_email", client_email),
        ("customer_number", customer_number),
    ]:
        _logger.info(f"{param_name}: {param}")

    assert offer_id is not None
    assert imei is not None
    assert client_email is not None
    assert customer_number is not None

    assert isinstance(email, str)
    assert isinstance(amount, int)
    assert isinstance(status, str)
    assert isinstance(id_type, str)
    assert isinstance(payment_model, str)
    assert isinstance(number_pattern, str)
    assert isinstance(offer_id, str)
    assert isinstance(imei, str)
    assert isinstance(sim_type, str)
    assert isinstance(device_batch, str)
    assert isinstance(sim_batch, str)
    assert isinstance(client_email, str)
    assert isinstance(customer_number, int)

    task_instance.xcom_push(key="email", value=email)
    task_instance.xcom_push(key="amount", value=amount)
    task_instance.xcom_push(key="status", value=status)
    task_instance.xcom_push(key="id_type", value=id_type)
    task_instance.xcom_push(key="payment_model", value=payment_model)
    task_instance.xcom_push(key="number_pattern", value=number_pattern)
    task_instance.xcom_push(key="offer_id", value=offer_id)
    task_instance.xcom_push(key="imei", value=imei)
    task_instance.xcom_push(key="sim_type", value=sim_type)
    task_instance.xcom_push(key="device_batch", value=device_batch)
    task_instance.xcom_push(key="sim_batch", value=sim_batch)
    task_instance.xcom_push(key="client_email", value=client_email)
    task_instance.xcom_push(key="customer_number", value=customer_number)

def prepare_new_sell_pospaid_params(**kwargs):
    task_instance: models.TaskInstance = kwargs["ti"]
    params: models.Params = kwargs["params"]

    email: str = params["email"]
    amount: int = params["amount"]
    status: str = params["status"]
    id_type: str = params["id_type"]
    payment_model: str = params["payment_model"]
    number_pattern: str = params["number_pattern"]
    offer_id: str = params["offer_id"]
    imei: str = params["imei"]
    sim_type: str = params["sim_type_pospaid"]
    device_batch: str = params["device_batch"]
    sim_batch: str = params["sim_batch"]
    customer_number: int = params["customer_number"]
    reference_number: int = params["reference_number"]
    reference_name: str = params["reference_name"]

    _logger.info("Parámetros de nueva venta prepago:")
    for param_name, param in [
        ("email", email),
        ("amount", amount),
        ("status", status),
        ("id_type", id_type),
        ("payment_model", payment_model),
        ("number_pattern", number_pattern),
        ("offer_id", offer_id),
        ("imei", imei),
        ("sim_type", sim_type),
        ("device_batch", device_batch),
        ("sim_batch", sim_batch),
        ("customer_number", customer_number),
        ("reference_number", reference_number),
        ("reference_name", reference_name),
    ]:
        _logger.info(f"{param_name}: {param}")

    assert offer_id is not None
    assert imei is not None
    assert customer_number is not None
    assert reference_number is not None
    assert reference_name is not None

    assert isinstance(email, str)
    assert isinstance(amount, int)
    assert isinstance(status, str)
    assert isinstance(id_type, str)
    assert isinstance(payment_model, str)
    assert isinstance(number_pattern, str)
    assert isinstance(offer_id, str)
    assert isinstance(imei, str)
    assert isinstance(sim_type, str)
    assert isinstance(device_batch, str)
    assert isinstance(sim_batch, str)
    assert isinstance(customer_number, int)
    assert isinstance(reference_number, int)
    assert isinstance(reference_name, str)

    task_instance.xcom_push(key="email", value=email)
    task_instance.xcom_push(key="amount", value=amount)
    task_instance.xcom_push(key="status", value=status)
    task_instance.xcom_push(key="id_type", value=id_type)
    task_instance.xcom_push(key="payment_model", value=payment_model)
    task_instance.xcom_push(key="number_pattern", value=number_pattern)
    task_instance.xcom_push(key="offer_id", value=offer_id)
    task_instance.xcom_push(key="imei", value=imei)
    task_instance.xcom_push(key="sim_type", value=sim_type)
    task_instance.xcom_push(key="device_batch", value=device_batch)
    task_instance.xcom_push(key="sim_batch", value=sim_batch)
    task_instance.xcom_push(key="customer_number", value=customer_number)
    task_instance.xcom_push(key="reference_number", value=reference_number)
    task_instance.xcom_push(key="reference_name", value=reference_name)

_dag_params = {
    "email": Param(
        default="example@sqasa.co",
        description="E-mail",
        type="string",
        format="idn-email",
        minLength=16,
        maxLength=100,
        title="E-mail",
        description_md="Ingrese su correo electrónico de dominio TELEFONICA o SQASA para el envío del resultado",
        # pattern=_email_regex_pattern,
    ),
    "business_operation": Param(
        description="Operación de negocio",
        type="string",
        enum=[
            "cambio_numero",
            "cambio_sim_card",
            "serializacion_cambio_sim_card",
            "venta_recurso",
            "pos_2_pre",
            "recarga",
            "desactivacion",
            "venta_nueva_prepago",
            "venta_nueva_pospago",
        ],
        values_display=_business_operation_values_display,
        title="Operación de negocio",
        description_md="Escoja la operación de negocio"
    ),
    "amount": Param(
        default=1,
        description="Cantidad de datos a generar",
        type="integer",
        minimum=1,
        title="Cantidad de datos",
        description_md="Ingrese la cantidad de registros a generar",
    ),
    "status": Param(
        default="2",
        description="Estado (Preactiva, Activa, Suspensión parcial, Suspensión total, Predesactiva)",
        type="string",
        enum=["1", "2", "3", "4", "9"],
        values_display={
            "1": "Preactiva",
            "2": "Activa",
            "3": "Suspensión parcial",
            "4": "Suspensión total",
            "9": "Pre-desactivada",
        },
        title="Estado",
        description_md=(
            "Ingrese el estado: Preactiva, Activa, Suspensión parcial, "
            "Suspensión total o Predesactiva"
        ),
    ),
    "payment_type": Param(
        default="0",
        description="Tipo de pago (Prepago o Pospago)",
        type="string",
        enum=["0", "1"],
        values_display={
            "0": "Prepago",
            "1": "Pospago",
        },
        title="Tipo de pago",
        description_md="Ingrese el tipo de pago: Prepago o Pospago",
    ),
    "client_type": Param(
        default="2",
        description="Tipo de cliente (B2C, B2B)",
        type="string",
        enum=["1", "2"],
        values_display={
            "1": "B2C",
            "2": "B2B",
        },
        title="Tipo de cliente",
        description_md="Ingrese el tipo de cliente: B2C o B2B",
    ),
    "id_type": Param(
        default="CC",
        description="Tipo de documento (CC, NIT, CE, PS, PPT)",
        type="string",
        enum=["CC", "CE", "PS", "31", "18"],
        values_display=_id_type_values_display,
        title="Tipo de documento",
        description_md="Ingrese el tipo de documento (CC, NIT, CE, PPT, Pasaporte)",
    ),
    "change_reason": Param(
        default="none",
        description="Motivo del cambio de SIM card",
        type="string",
        enum=[
            "dano",
            "cambio_tecnologia",
            "perdida_robo",
            "solicitud_cliente",
        ],
        values_display=_change_reason_values_display,
        title="Motivo cambio de SIM card",
        description_md="Escoja el motivo del cambio de SIM card (solo aplica para CAMBIO DE SIM CARD)",
    ),
    "order_id": Param(
        description="Order ID",
        type=["integer", "null"],
        title="Order ID",
        description_md="Ingrese el ID de la orden generada en el Cambio de SIM card",
        pattern=r"^\d+$",
    ),
    "payment_model": Param(
        default="inmediato",
        description="Modelo de pago",
        type="string",
        enum=[
            "inmediato",
            "ciclo",
            "sincronizacion_ar",
        ],
        values_display={
            "inmediato": "Inmediatamente(En sitio)",
            "ciclo": "Posterior a la activación(Max. próximo ciclo)",
            "sincronizacion_ar": "Envío a módulo de cartera",
        },
        title="Modelo de pago",
    ),
    "service_account_type": Param(
        default="1",
        description="Tipo de servicio (Móvil, Banda ancha, Fijo)",
        type="string",
        enum=["1", "2", "3"],
        values_display={
            "1": "Móvil",
            "2": "Banda ancha",
            "3": "Fijo",
        },
        title="Tipo de servicio",
        description_md="Ingrese el tipo de servicio: Móvil, Banda ancha o Fijo",
    ),
    "number_pattern": Param(
        default="315",
        description="Patrón del número (315, 316, 317, 318)",
        type="string",
        enum=[
            "315",
            "316",
            "317",
            "318",
        ],
        values_display={
            "315": "315 (recomendado)",
        },
        title="Patrón número de servicio",
        description_md="Ingrese el patrón del nuevo número de servicio (solo aplica para CAMBIO DE NÚMERO MÓVIL)",
    ),
    "sell_reason": Param(
        default="6",
        description=(
            "Razón de venta: Migración, Plan Empleado, Ventas en tienda, "
            "Corrección recursos, Recursos de campaña, Renovación o "
            "Reposición, Seguro por daños, Seguro por robo, Venta a cuotas, "
            "Ventana de recursos por página WEB, Otros"
        ),
        type="string",
        enum=["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
        values_display=_sell_reason_values_display,
        title="Razón de venta",
        description_md=(
            "Ingrese la razón de venta: Migración, Plan Empleado, Ventas en "
            "tienda, Corrección recursos, Recursos de campaña, Renovación o "
            "Reposición, Seguro por daños, Seguro por robo, Venta a cuotas, "
            "Ventana de recursos por página WEB, Otros"
        )
    ),
    "offer_name": Param(
        description="Nombre de Oferta",
        type=["string", "null"],
        title="Nombre de Oferta",
        description_md="Ingrese el nombre de la oferta",
    ),
    "device_color": Param(
        description="Color del dispositivo",
        type=["string", "null"],
        title="Color del dispositivo",
        description_md="Ingrese el color del dispositivo",
    ),
    "effective_method": Param(
        default="inmediatamente",
        description="Método efectivo (TODO: options)",
        type=["string", "null"],
        enum=[
            "inmediatamente",
            "proximo_ciclo_facturacion",
        ],
        values_display=_effective_method_values_display,
        title="Método efectivo",
        description_md="Ingrese el método efectivo (TODO: options)",
    ),
    "customer_number": Param(
        default=3150000000,
        description="Número de celular del cliente",
        type=["integer", "null"],
        minimum=3000000000,
        maximum=3999999999,
        title="Número del cliente",
        description_md="Ingrese el número de celular del cliente",
        pattern=r"^3\d{9}$",
    ),
    "reference_name": Param(
        description="Nombre de referencia",
        type=["string", "null"],
        title="Nombre de referencia",
        description_md="Ingrese el nombre de referencia",
    ),
    "reference_number": Param(
        default=3150000000,
        description="Número de referencia del cliente",
        type=["integer", "null"],
        minimum=3000000000,
        maximum=3999999999,
        title="Número de referencia",
        description_md="Ingrese el número de referencia del cliente",
        pattern=r"^3\d{9}$",
    ),
    "pos2pre_change_reason": Param(
        default="queja_cliente",
        description="Razón del cambio",
        type=["string", "null"],
        enum=[
            "queja_cliente",
            "retencion",
            "solicitud_cliente",
        ],
        values_display=_pos2pre_change_reason_values_display,
        title="Razón del cambio",
        description_md="Ingrese la razón del cambio",
    ),
    "client_email": Param(
        default="example@gmail.com",
        description="E-mail del cliente",
        type=["string", "null"],
        format="idn-email",
        maxLength=100,
        title="E-mail del cliente",
        description_md="Ingrese el correo electrónico del cliente",
    ),
    "cancellation_reason": Param(
        default="mejor_oferta_precio",
        description="Motivo de la cancelación",
        type=["string", "null"],
        enum=[
            "mejor_oferta_precio",
            "precio_terminal",
            "mejor_oferta_producto",
            "renovacion_beneficio",
            "cobro_reconexion",
            "pago_no_aplicado",
            "no_recibe_factura",
            "cobro_no_reconocido",
            "inconforme_proceso_venta",
            "incumplimiento_promesa",
            "inconforme_servicio_tecnico",
            "radicacion_tercero_ce",
            "fallecimiento",
            "reduccion_costos",
            "cierre_negocio",
            "calidad_funcionamiento",
            "sin_cobertura",
        ],
        values_display=_cancellation_reason_values_display,
        title="Motivo de cancelación",
        description_md="Ingrese el motivo de cancelación",
    ),
    "deactivation_expiration_time": Param(
        default="inmediatamente",
        description="Tiempo de expiración de la desactivación",
        type=["string", "null"],
        enum=[
            "expira_siguiente_ciclo",
            "inmediatamente",
            "vigente_desde",
        ],
        values_display=_deactivation_expiration_time_values_display,
        title="Tiempo expiración desactivación",
        description_md="Ingrese el tiempo de expiración de la desactivación",
    ),
    "offer_id": Param(
        default="0000",
        description="ID de la oferta",
        type=["string", "null"],
        title="ID de la oferta",
        description_md="Ingrese el ID de la oferta",
        pattern=r"^\d{4}$",
    ),
    "imei": Param(
        description="IMEI",
        type=["string", "null"],
        title="IMEI",
        description_md="Ingrese el código IMEI",
        pattern=r"^\d{15}$",
    ),
    "sim_type_prepaid": Param(
        default="multi_simcard_pre_lte",
        description="Tipo de SIM prepago",
        type="string",
        enum=[
            "multi_simcard_pre_lte",
            "esim_card_thales_digitalpre_orq",
            "esim_card_thales_digitalpre_orq",
            "esim_card_thales_digitalpos_qr_generico",
            "esim_card_digital_qr_pre",
            "esim_card_digital_qr_pos",
            "esim_card_evoucherpre_qr_piloto",
            "multi_simcard_pre_lte",
            "sim_card_gemal_pre_lte",
            "sim_card_valid_pre_lte",
            "sim_pre_gd",
            "sim_pre_idemia",
            "sim_pre_idem_lte",
            "sim_pre_thales",
            "sim_pre_valid",
        ],
        values_display=_sim_type_prepaid_values_display,
        title="Tipo de SIM prepago",
        description_md="Ingrese el tipo de SIM prepago",
    ),
    "sim_type_pospaid": Param(
        default="multi_simcard_pospa_lte",
        description="Tipo de SIM pospago",
        type="string",
        enum=[
            "multi_simcard_pospa_lte",
            "esim_card_digital_qr_pos",
            "esim_card_evoucherpos_qr_digital",
            "esim_card_evoucherpre_qr_piloto",
            "sim_card_gemal_pos_lte",
            "sim_card_valid_pos_lte",
            "sim_pos_gd",
            "sim_pos_idemia",
            "sim_pos_idem_lte",
            "sim_pos_thales",
            "sim_pos_valid",
        ],
        values_display=_sim_type_pospaid_values_display,
        title="Tipo de SIM pospago",
        description_md="Ingrese el tipo de SIM pospago",
    ),
    "device_batch": Param(
        default="NUEVO",
        description="Lote de dispositivo",
        type="string",
        enum=[
            "nuevo",
            "triangulado",
            "pruebas",
            "tercero",
        ],
        values_display=_device_batch_values_display,
        title="Lote de dispositivo",
        description_md="Ingrese el lote del dispositivo",
    ),
    "sim_batch": Param(
        default="NUEVO",
        description="Lote de SIM",
        type="string",
        enum=[
            "nuevo",
            "pruebas",
        ],
        values_display=_sim_batch_values_display,
        title="Lote de SIM",
        description_md="Ingrese el lote de la SIM",
    ),
}

_dag_description = (
    "Consulta la información necesaria para ejecutar pruebas automáticas en "
    "la plataforma de FullStack."
)

with DAG(
    dag_id="generacion_insumos_todos_cloud",
    description=_dag_description,
    default_args={
        "owner": "airflow",
    },
    start_date=dt.datetime(year=2025, month=11, day=5, tzinfo=_timezone),
    schedule=None,
    params=_dag_params,
    tags=["prueba", "automatica", "generacion", "insumos", "todos", "sequoia", "cloud"],
):

    # Task Definition

    determine_business_operation_task = BranchPythonOperator(
        task_id="determinar_operacion_de_negocio",
        python_callable=determine_business_operation,
    )

    # Business operations

    prepare_recharge_params_task = PythonOperator(
        task_id="preparar_parametros_recarga",
        python_callable=prepare_recharge_params,
    )

    prepare_simcard_change_params_task = PythonOperator(
        task_id="preparar_parametros_cambio_sim_card",
        python_callable=prepare_simcard_change_params,
    )

    prepare_serialization_simcard_change_params_task = PythonOperator(
        task_id="preparar_parametros_serializacion_cambio_sim_card",
        python_callable=prepare_serialization_simcard_change_params,
    )

    prepare_number_change_params_task = PythonOperator(
        task_id="preparar_parametros_cambio_numero",
        python_callable=prepare_number_change_params,
    )

    prepare_mobile_sell_params_task = PythonOperator(
        task_id="preparar_parametros_venta_recurso",
        python_callable=prepare_mobile_sell_params,
    )

    prepare_pos2pre_params_task = PythonOperator(
        task_id="preparar_parametros_pos2pre",
        python_callable=prepare_pos2pre_params,
    )

    prepare_deactivation_params_task = PythonOperator(
        task_id="preparar_parametros_desactivacion",
        python_callable=prepare_deactivation_params,
    )

    prepare_new_sell_prepaid_params_task = PythonOperator(
        task_id="preparar_parametros_venta_nueva_prepago",
        python_callable=prepare_new_sell_prepaid_params,
    )

    prepare_new_sell_pospaid_params_task = PythonOperator(
        task_id="preparar_parametros_venta_nueva_pospago",
        python_callable=prepare_new_sell_pospaid_params,
    )

    invalid_business_operation_task = EmptyOperator(
        task_id="operacion_de_negocio_invalida",
    )

    # Trigger corresponding DAG

    trigger_recharge_task = TriggerDagRunOperator(
        task_id="activar_DAG_recarga",
        trigger_dag_id="generacion_insumos_recarga_cloud",
        conf={
            "email": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_recarga', key='email') }}",
            "amount": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_recarga', key='amount') }}",
            "status": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_recarga', key='status') }}",
            "payment_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_recarga', key='payment_type') }}",
            "client_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_recarga', key='client_type') }}",
        },
    )

    trigger_simcard_change_task = TriggerDagRunOperator(
        task_id="activar_DAG_cambio_simcard",
        trigger_dag_id="generacion_insumos_cambio_sim_cloud",
        conf={
            "email": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_sim_card', key='email') }}",
            "amount": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_sim_card', key='amount') }}",
            "status": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_sim_card', key='status') }}",
            "client_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_sim_card', key='client_type') }}",
            "payment_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_sim_card', key='payment_type') }}",
            "change_reason": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_sim_card', key='change_reason') }}",
            "payment_model": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_sim_card', key='payment_model') }}",
        },
    )

    trigger_serialization_simcard_change_task = TriggerDagRunOperator(
        task_id="activar_DAG_serializacion_cambio_simcard",
        trigger_dag_id="generacion_insumos_serializacion_cambio_sim_cloud",
        conf={
            "email": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_serializacion_cambio_sim_card', key='email') }}",
            "amount": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_serializacion_cambio_sim_card', key='amount') }}",
            "order_id": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_serializacion_cambio_sim_card', key='order_id') }}",
        },
    )

    trigger_number_change_task = TriggerDagRunOperator(
        task_id="activar_DAG_cambio_numero",
        trigger_dag_id="generacion_insumos_cambio_numero_cloud",
        conf={
            "email": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_numero', key='email') }}",
            "amount": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_numero', key='amount') }}",
            "status": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_numero', key='status') }}",
            "client_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_numero', key='client_type') }}",
            "payment_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_numero', key='payment_type') }}",
            "payment_model": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_numero', key='payment_model') }}",
            "number_pattern": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_cambio_numero', key='number_pattern') }}",
        },
    )

    trigger_mobile_sell_task = TriggerDagRunOperator(
        task_id="activar_DAG_venta_recurso",
        trigger_dag_id="generacion_insumos_venta_recurso_cloud",
        conf={
            "email": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_recurso', key='email') }}",
            "amount": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_recurso', key='amount') }}",
            "status": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_recurso', key='status') }}",
            "payment_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_recurso', key='payment_type') }}",
            "client_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_recurso', key='client_type') }}",
            "payment_model": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_recurso', key='payment_model') }}",
            "sell_reason": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_recurso', key='sell_reason') }}",
            "offer_name": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_recurso', key='offer_name') }}",
            "device_color": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_recurso', key='device_color') }}",
        }
    )

    trigger_pos2pre_task = TriggerDagRunOperator(
        task_id="activar_DAG_pos2pre",
        trigger_dag_id="generacion_insumos_pos2pre_cloud",
        conf={
            "email": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_pos2pre', key='email') }}",
            "amount": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_pos2pre', key='amount') }}",
            "status": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_pos2pre', key='status') }}",
            "client_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_pos2pre', key='client_type') }}",
            "client_email": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_pos2pre', key='client_email') }}",
            "effective_method": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_pos2pre', key='effective_method') }}",
            "customer_number": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_pos2pre', key='customer_number') }}",
            "pos2pre_change_reason": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_pos2pre', key='pos2pre_change_reason') }}",
        }
    )

    trigger_deactivation_task = TriggerDagRunOperator(
        task_id="activar_DAG_desactivacion",
        trigger_dag_id="generacion_insumos_desactivacion_cloud",
        conf={
            "email": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_desactivacion', key='email') }}",
            "amount": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_desactivacion', key='amount') }}",
            "status": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_desactivacion', key='status') }}",
            "client_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_desactivacion', key='client_type') }}",
            "payment_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_desactivacion', key='payment_type') }}",
            "cancellation_reason": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_desactivacion', key='cancellation_reason') }}",
            "deactivation_expiration_time": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_desactivacion', key='deactivation_expiration_time') }}",
        }
    )

    trigger_new_sell_prepaid_task = TriggerDagRunOperator(
        task_id="activar_DAG_venta_nueva_prepago",
        trigger_dag_id="generacion_insumos_venta_nueva_prepago_cloud",
        conf={
            "email": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='email') }}",
            "amount": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='amount') }}",
            "status": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='status') }}",
            "id_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='id_type') }}",
            "payment_model": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='payment_model') }}",
            "number_pattern": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='number_pattern') }}",
            "offer_id": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='offer_id') }}",
            "imei": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='imei') }}",
            "sim_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='sim_type') }}",
            "device_batch": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='device_batch') }}",
            "sim_batch": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='sim_batch') }}",
            "client_email": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='client_email') }}",
            "customer_number": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_prepago', key='customer_number') }}",
        },
    )

    trigger_new_sell_pospaid_task = TriggerDagRunOperator(
        task_id="activar_DAG_venta_nueva_pospago",
        trigger_dag_id="generacion_insumos_venta_nueva_pospago_cloud",
        conf={
            "email": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='email') }}",
            "amount": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='amount') }}",
            "status": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='status') }}",
            "id_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='id_type') }}",
            "payment_model": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='payment_model') }}",
            "number_pattern": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='number_pattern') }}",
            "offer_id": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='offer_id') }}",
            "imei": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='imei') }}",
            "sim_type": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='sim_type') }}",
            "device_batch": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='device_batch') }}",
            "sim_batch": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='sim_batch') }}",
            "customer_number": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='customer_number') }}",
            "reference_number": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='reference_number') }}",
            "reference_name": "{{ task_instance.xcom_pull(task_ids='preparar_parametros_venta_nueva_pospago', key='reference_name') }}",
        }
    )

    # DAG Flow Definition

    # Prep work

    (
        determine_business_operation_task >>
        [
            prepare_recharge_params_task,
            prepare_simcard_change_params_task,
            prepare_serialization_simcard_change_params_task,
            prepare_number_change_params_task,
            prepare_mobile_sell_params_task,
            prepare_pos2pre_params_task,
            prepare_deactivation_params_task,
            prepare_new_sell_prepaid_params_task,
            prepare_new_sell_pospaid_params_task,
            invalid_business_operation_task,
        ]
    )

    # Recharge

    (
        prepare_recharge_params_task >>
        trigger_recharge_task
    )

    # SIM Card change

    (
        prepare_simcard_change_params_task >>
        trigger_simcard_change_task
    )

    # Serialization SIM Card change

    (
        prepare_serialization_simcard_change_params_task >>
        trigger_serialization_simcard_change_task
    )

    # Number change

    (
        prepare_number_change_params_task >>
        trigger_number_change_task
    )

    # Mobile sell

    (
        prepare_mobile_sell_params_task >>
        trigger_mobile_sell_task
    )

    # Pos2Pre

    (
        prepare_pos2pre_params_task >>
        trigger_pos2pre_task
    )

    # Deactivation

    (
        prepare_deactivation_params_task >>
        trigger_deactivation_task
    )

    # Venta nueva prepago

    (
        prepare_new_sell_prepaid_params_task >>
        trigger_new_sell_prepaid_task
    )

    # Venta nueva pospago

    (
        prepare_new_sell_pospaid_params_task >>
        trigger_new_sell_pospaid_task
    )
