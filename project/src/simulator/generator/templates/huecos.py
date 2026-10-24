"""Listas para rellenar los huecos de las plantillas (``{envase}``, ``{lugar}``, ...)."""

# Cómo puede aparecer cada marca dentro del texto (clave canónica -> variantes; las repetidas
# pesan más). Una marca sin entrada aquí usa los alias de brands.yaml.
ALIAS_TEXTO = {
    "postobon": ["Postobón", "Postobón", "Postobon", "Postobón"],
    "cocacola": ["CocaCola", "Coca-Cola", "Coca Cola", "Coca Cola", "Coca", "Coca-Cola",
                 "Cocacola", "Coca-cola", "Coca Cola"],
    "sprite": ["Sprite", "Sprite", "Sprite"],
    "pepsi": ["Pepsi", "Pepsi", "Pepsi", "Pepsi Cola"],
    "colombiana": ["Colombiana", "Colombiana", "Colombiana"],
}

VARIANTES = {
    "postobon": ["de manzana", "de uva", "de naranja", "de pera", "de piña", "de litro y medio",
                 "de tamarindo", "de mandarina", "de lima limón", "de litro", "de 250 ml"],
    "cocacola": ["Zero", "sin azúcar", "original", "clásica", "de vidrio", "de lata",
                 "de 250 ml", "Light", "de medio litro", "de tres litros", "retornable"],
    "sprite": ["de limón", "Zero", "de lata", "de dos litros", "de vidrio",
               "sin azúcar", "de medio litro", "retornable", "de litro y medio"],
    "pepsi": ["Zero", "Black", "de lata", "de dos litros", "de vidrio",
              "Light", "de medio litro", "retornable", "de litro y medio"],
    "colombiana": ["de litro y medio", "en botella", "de vidrio", "de lata", "personal",
                   "de 250 ml", "de medio litro", "de tres litros", "retornable", "de dos litros"],
}
# Para marcas nuevas de brands.yaml que aún no tengan variantes propias.
VARIANTES_GENERICAS = ["original", "Zero", "de vidrio", "de lata", "de litro y medio", "retornable"]

ENVASES = [
    "una {m}", "una {m} personal", "una botella de vidrio de {m}", "una lata de {m}",
    "un litro y medio de {m}", "una botella de {m} de dos litros", "una {m} de 400 ml",
    "un vaso de {m}", "una {m} con hielo", "una jarra de {m}", "una {m}", "una {m}",
]
ENVASES += [
    "una {m} heladita", "una {m} de medio litro", "una {m} retornable", "dos {m}", "una {m} bien fría",
    "una botellita de {m}", "un vasito de {m}", "una {m} de tres litros", "una {m} pequeña",
    "una {m} sin hielo", "una {m} con limón", "un six pack de {m}", "una caja de {m}",
    "una {m} de 250 ml", "una {m} de las grandes", "un combo con {m}", "un vaso de {m} con mucho hielo",
]

COMIDAS = [
    "empanadas", "arepa con queso", "bandeja paisa", "pizza", "hamburguesa", "sancocho",
    "pollo asado", "salchipapa", "chicharrón", "perro caliente", "buñuelos", "pandebono",
    "tamales", "ajiaco", "papas fritas", "pan con queso", "lechona", "mondongo", "arroz con pollo",
    "patacones", "carne asada", "natilla y buñuelos", "un almuerzo corrientazo", "fritanga",
]
COMIDAS += [
    "ajiaco santafereño", "cazuela de mariscos", "arroz con coco", "pescado frito", "sudado de pollo",
    "empanadas de pipián", "arepa de huevo", "carimañolas", "bollo con queso", "almojábanas",
    "pan de bono", "tostadas con hogao", "changua", "calentado", "mute santandereano",
    "cuchuco", "fritos del domingo", "chorizo con arepa", "tacos", "hot dog", "nuggets",
    "alitas picantes", "lasaña", "espagueti", "sándwich", "choripán", "mazorca desgranada",
    "tamal tolimense", "sopa de arroz", "frijoles con garra", "morcilla", "papa criolla",
    "ceviche", "arepa boyacense", "cocadas", "obleas", "pizza de piña", "croquetas", "ensalada",
]

LUGARES = [
    "en la tienda de la esquina", "en el estadio", "en una fiesta", "en el trabajo", "en el bus",
    "en la finca", "en la playa", "en Medellín", "en Bogotá", "en Cali", "en Barranquilla",
    "en el colegio", "en la universidad", "en el parque", "en un paseo de olla", "en el cine",
    "en un asado", "en la oficina", "en el centro comercial", "en el río", "en el Eje Cafetero",
    "en Cartagena", "en la casa de mi abuela", "en el barrio", "en la panadería",
    "en un restaurante", "en una cantina", "en el carro", "en el terminal", "en la cancha",
]
LUGARES += [
    "en el parque Arví", "en Guatapé", "en San Andrés", "en Santa Marta", "en Bucaramanga",
    "en Pereira", "en Manizales", "en Cúcuta", "en Villavicencio", "en Pasto", "en Tunja",
    "en el Parque Tayrona", "en la plaza de mercado", "en la terraza de mi casa", "en la sala de espera",
    "en el aeropuerto", "en la fila del banco", "en el metro", "en el Transmilenio", "en el gimnasio",
    "en el hospital", "en la sede del trabajo", "en la heladería", "en el piso de arriba",
    "en una chiva", "en un picnic", "en el balcón", "en el patio", "en la cafetería de la u",
    "en un bar de esquina", "en el mirador", "en el lago", "en una tienda de barrio en Chía",
    "en la montaña", "en la vereda", "en el pueblo", "en el tercer piso de la oficina",
]

MOMENTOS = [
    "en la mañana", "después de almorzar", "a media tarde", "en la noche", "después del partido",
    "el domingo", "en el descanso", "antes de dormir", "en vacaciones", "con este calor",
    "en el trancón", "en diciembre", "en Semana Santa", "después del gimnasio", "en el recreo",
    "a la hora del almuerzo", "al salir del trabajo", "un viernes cualquiera", "en la madrugada",
    "después de comer",
]
MOMENTOS += [
    "a las tres de la tarde", "a las seis de la mañana", "en pleno almuerzo", "mientras veía la novela",
    "durante el partido de la Selección", "mientras estudiaba", "en la hora del café", "al terminar el turno",
    "después de la misa", "mientras hacía el aseo", "en el puente festivo", "en la noche de películas",
    "en la fiesta de fin de año", "antes de la entrevista", "después del parcial", "en la sobremesa",
    "con la lluvia afuera", "esta mañana", "ayer en la tarde", "el sábado en la noche",
    "mientras esperaba el bus", "en la madrugada del lunes", "a la salida del colegio",
]

PERSONAS = [
    "mi mamá", "mi papá", "mi abuela", "mi abuelo", "mi esposa", "mi esposo", "mi novia",
    "mi novio", "mi hermano", "mi hermana", "mi tío", "mi tía", "mi jefe",
    "mi compañero de trabajo", "mi mejor amigo", "mi vecina", "mi primo",
]
PERSONAS += [
    "mi suegra", "mi suegro", "mi cuñado", "mi cuñada", "mi sobrino", "mi sobrina", "mi nieto",
    "mi ahijado", "mi profesora", "mi compadre", "mi comadre", "mi vecino", "mi amiga del colegio",
    "mi parcero", "mi mejor amiga", "mi hija", "mi hijo", "el tío Jorge", "la abuela Rosa",
    "mi socio", "mi crush", "mi ex", "mi prima", "mi padrino",
]

PRECIOS = ["mil pesos", "dos mil pesos", "tres lucas", "mil quinientos pesos", "quinientos pesos",
           "cinco mil pesos", "dos lucas", "tres mil quinientos", "cuatro mil pesos", "mil doscientos"]
CLIMAS = ["con este solazo", "bajo la lluvia", "con 35 grados", "en pleno aguacero", "con el frío de Bogotá",
          "con este calorón", "en plena ola de calor", "con el sol a pleno", "en un día nublado",
          "con la brisa de la tarde", "en el mediodía más caliente", "cuando hace un bochorno tremendo"]
EVENTOS = ["en el cumpleaños de mi sobrino", "en el matrimonio de mi prima", "en la primera comunión",
           "en la novena de aguinaldos", "en el bazar del colegio", "en la final del fútbol",
           "en la Feria de las Flores", "en el Carnaval de Barranquilla", "en el día sin IVA",
           "en el grado de mi hermana", "en la reunión de exalumnos", "en el baby shower",
           "en la fiesta de quince", "en el paseo de la empresa", "en la despedida de soltero",
           "en la cena de Nochebuena", "en la celebración del 20 de julio", "en la inauguración del negocio",
           "en el bautizo del bebé", "en el festival vallenato", "en el puente de reyes"]
ADJ_POS = ["refrescante", "deliciosa", "perfecta", "burbujeante", "inigualable", "adictivamente rica",
           "bien dulcecita", "súper fresca", "espectacular", "una joya", "un clásico", "bien gaseosa",
           "buenísima", "increíble", "ideal para compartir", "una maravilla"]
ADJ_NEG = ["empalagosa", "insípida", "artificial", "desabrida", "demasiado dulce", "una pesadilla",
           "un jarabe con gas", "nauseabunda", "pesada", "desagradable", "un veneno", "horrible",
           "metálica", "medicinal", "insoportable", "una porquería"]

# Aperturas extra que se suman a las de cada categoría (menos "informativo", que trae las suyas).
AP_EXTRA = ["Ayer ", "Hoy ", "Ojalá me lean: ", "Hilo: ", "Spoiler: ", "Entre amigos: ", "Les cuento algo: ",
            "No sé si les pasa, pero ", "Dicho sea de paso, ", "Mi aporte del día: ", "Pa la historia: ",
            "A ver, escuchen: ", "Parce, ", "Mano, ", "Uy, ", "Oigan, ", "Nada, ", "Cómo así, "]
