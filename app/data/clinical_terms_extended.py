# Diccionario clínico base ampliado
# Puedes expandir esto sin tocar el backend

CLINICAL_TERMS = {
    "sintomas_generales": [
        ("fiebre", "R50"),
        ("fatiga", "R53"),
        ("debilidad", "R53"),
        ("perdida de peso", "R63"),
        ("ganancia de peso", "R63"),
        ("mareo", "R42"),
        ("vertigo", "R42"),
        ("malestar general", "R68"),
    ],

    "cardiovascular": [
        ("dolor de pecho", "I20"),
        ("opresion toracica", "I20"),
        ("angina", "I20"),
        ("infarto", "I21"),
        ("palpitaciones", "R00"),
        ("sincope", "R55"),
        ("disnea", "R06"),
        ("edema", "R60"),
        ("presion alta", "I10"),
        ("hipertension", "I10"),
        ("colesterol alto", "E78"),
        ("hiperlipidemia", "E78"),
    ],

    "endocrino": [
        ("diabetes", "E11"),
        ("azucar alta", "R73"),
        ("hiperglucemia", "R73"),
        ("poliuria", "R35"),
        ("polidipsia", "R63"),
        ("obesidad", "E66"),
        ("resistencia a la insulina", "E88"),
        ("tiroides", "E03"),
        ("hipotiroidismo", "E03"),
        ("hipertiroidismo", "E05"),
    ],

    "dolor": [
        ("dolor", "R52"),
        ("dolor cronico", "R52"),
        ("dolor agudo", "R52"),
        ("dolor neuropatico", "G63"),
        ("quemazon", "R52"),
        ("hormigueo", "R20"),
    ],

    "traumatologia": [
        ("dolor lumbar", "M54"),
        ("lumbalgia", "M54"),
        ("dolor de rodilla", "M25"),
        ("dolor de hombro", "M25"),
        ("fractura", "S52"),
        ("esguince", "S93"),
        ("luxacion", "S53"),
    ],

    "pediatria": [
        ("nino con fiebre", "R50"),
        ("fiebre en ninos", "R50"),
        ("infeccion respiratoria", "J06"),
        ("tos en ninos", "R05"),
        ("diarrea en ninos", "A09"),
    ],

    "respiratorio": [
        ("tos", "R05"),
        ("hemoptisis", "R04"),
        ("sibilancias", "R06"),
        ("cianosis", "R23"),
        ("neumonia", "J18"),
        ("asma", "J45"),
    ],
}
