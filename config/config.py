
#-------------variables hilado-----------------------


ASPECTO_MOV_HILADO = [
    "SUSTENTO COSTO HILADO",
    "1 Hilado Enviado",
    "2 Hilado Devuelto",
]

ASPECTO_CONSUMO_HILADO =[
    "Hilado Consumido",
    "Hilado Merma"
]

COLS_MOV_HILADO = [
    "OP Referencial",
    "Cod Hilado", #lower en la tabla
    "Partida",
    "Fecha", #lower en la tabla
    "Tipo", #tipo_mov en la tabla
    "Cliente",
    "Aspecto",
    "Proveedor", #lower en tabla
    "Transaccion", #lower en la tabla
    "Num.Mov.", #num_mov en la tabla
    "Guia", #lower en la tabla
    "OC", #lower en la tabla
    "Factura", #lower en la tabla
    "Kgs", #lower en la tabla
    "dolares",
    "cu_usd",
    "Soles", #lower en la tabla
    "cu_soles"

]

COLS_ALM_HILADO = [
    "Cod Hilado",
    "Hilado", #descripcion en la tabla
    #"Lote", #lower en la tabla
]

COLS_CONSUMO_HILADO = [
    "OP Referencial",
    "Cod Hilado", #lower en la tabla
    "Fecha", #lower en la tabla
    "Tipo",
    "Cliente", #lower en la tabla
    "Aspecto",
    "Proveedor", #lower en tabla
    "Transaccion", #lower en la tabla
    "Num.Mov.",
    "Guia", #lower en la tabla
    "OC", #lower en la tabla
    "Factura", #lower en la tabla
    "Kgs", #lower en la tabla
    "dolares",
    "cu_usd",
    "Soles", #lower en la tabla
    "cu_soles"
]

#-------------variables de tejeduria-----------------

COLS_ALM_TEJEDURIA = [
    "Partida",
    "Cod_Tela",
    "Des_Tela",
    "Des_Comb", #cambiar por combo en la tabla
    "Cod_Color",
    "Des_Color"
]

ASPECTO_MOV_TEJEDURIA =[
    "Tejeduria",
    "CONFECCION",
    "Tintoreria Telas",
    "CONFECCION"
    #"Hilado Consumido",
    #"Hilado Merma"
]

COLS_MOV_TEJEDURIA = [
    "OP Referencial",
    "Partida",
    "Cod_Tela",
    "Des_Tela",
    "Des_Comb", #cambiar por combo en la tabla
    "Cod_Color",
    "Des_Color",
    "Num.Mov.",
    "orden",
    "Aspecto",
    "Medida",
    "Tipo",
    "Transaccion",
    "Cliente",
    "Proveedor",
    "Fecha",
    "Guia",
    "OC",
    "Factura",
    "Kgs",
    "dolares",
    "cu_usd"
]

#-------------------variables resumen-transacciones----------------

COD_DICT = {
    "D03":"Hilado enviado a tejeduría",
    "P20":"Hilado enviado a tejeduría",
    "Y20":"Hilado enviado a tejeduría",
    "D30":"Hilado sin valor comercial",
    "D39":"Hilado transferencia Multitech",
    "L20":"Envio de hilado a tintorería",
    "L34":"Servicio teñido de hilado",
    "R03":"Servicio teñido de hilado",
    "R04":"Servicio teñido de hilado",
    "X18":"Envio de hilado a tintorería",
    "X34":"Servicio teñido de hilado",
    "X69":"Servicio teñido de hilado",
    "Y18":"Envio de hilado a tintorería",
    "R01":"Compra de hilado",
    "X24":"Compra de hilado",
    "X45":"Compra de hilado",
    "X55":"Compra de hilado",
    "Y22":"Compra de hilado",
    "Y42":"Servicio de reenconado",
    "Y52":"Servicio de reenconado",
    "JE4C":"Hilado consumido en tejeduría",
    "JE4M":"Hilado merma de tejeduría",
    "DCM":"Envio de tela a CMT",
    "TCM":"Envio de tela a CMT",
    "JE4":"Servicio de tejeduría",
    "JS8":"Envio de tela a tintorería",
    "JST":"Servicio de post-teñido",
    "K61":"Envio de tela a tintorería",
    "K64":"Envio de tela a tintorería",
    "C06":"Servicio de post-teñido (estampado-sublimado)",
    "C25":"Servicio de post-teñido (estampado-sublimado)",
    "C08":"Servicio de post-teñido (estampado-sublimado)",
    "C14":"Servicio de post-teñido (estampado-sublimado)",
    "D77":"Compra de tela",
    "J01":"Servicio de tintorería",
    "R08":"Servicio de post-teñido",
    "T21":"Servicio de tintorería",
    "T22":"Servicio de tintorería",
    "T26":"Compra de tela",
    "T28":"Compra de tela",
    "T93":"Servicio de tintorería",
    "T94":"Servicio de tintorería",
    "TS2":"Transferencia de partidas",
    "Z9E":"Compra de tela"
}