#!/usr/bin/env python3
"""
Generador de comentarios sintéticos sobre gaseosas colombianas (v2, plantillas ampliadas).

Uso:
    python generar_comentarios.py                          # 2.000.000 filas
    python generar_comentarios.py -n 500000 -o prueba.csv
    python generar_comentarios.py --categoria --ruido 0.25 --seed 7
    python generar_comentarios.py --unico                  # sin duplicados exactos

Cómo funciona:
  apertura (según categoría) + frase central (con huecos que se rellenan al azar:
  envase, comida, lugar, momento, persona, variante) + 1-2 reacciones + cierre.
  Además se varía el nombre de la marca en el texto (Coca-Cola, la Coca, Postobón...)
  y se aplica "ruido" de redes sociales (typos, mayúsculas, alargues, jajaja...).
  La columna `marca` siempre queda con el nombre canónico.
"""
import argparse
import csv
import random
import re
import time

MARCAS = ["Postobon", "CocaCola", "Sprite", "Pepsi", "Colombiana"]

# Cómo puede aparecer la marca dentro del texto (el primero es el más frecuente)
ALIAS = {
    "Postobon": ["Postobon", "Postobon", "Postobón", "Postobón"],
    "CocaCola": ["CocaCola", "Coca-Cola", "Coca Cola", "Coca Cola", "Coca", "Coca-Cola"],
    "Sprite": ["Sprite", "Sprite", "Sprite"],
    "Pepsi": ["Pepsi", "Pepsi", "Pepsi"],
    "Colombiana": ["Colombiana", "Colombiana", "Colombiana"],
}

VARIANTES = {
    "Postobon": ["de manzana", "de uva", "de naranja", "de pera", "de piña", "de litro y medio"],
    "CocaCola": ["Zero", "sin azúcar", "original", "clásica", "de vidrio", "de lata"],
    "Sprite": ["de limón", "Zero", "de lata", "de dos litros", "de vidrio"],
    "Pepsi": ["Zero", "Black", "de lata", "de dos litros", "de vidrio"],
    "Colombiana": ["de litro y medio", "en botella", "de vidrio", "de lata", "personal"],
}

# ----------------------------------------------------------------- Huecos
ENVASES = [
    "una {m}", "una {m} personal", "una botella de vidrio de {m}", "una lata de {m}",
    "un litro y medio de {m}", "una botella de {m} de dos litros", "una {m} de 400 ml",
    "un vaso de {m}", "una {m} con hielo", "una jarra de {m}", "una {m}", "una {m}",
]
COMIDAS = [
    "empanadas", "arepa con queso", "bandeja paisa", "pizza", "hamburguesa", "sancocho",
    "pollo asado", "salchipapa", "chicharrón", "perro caliente", "buñuelos", "pandebono",
    "tamales", "ajiaco", "papas fritas", "pan con queso", "lechona", "mondongo", "arroz con pollo",
    "patacones", "carne asada", "natilla y buñuelos", "un almuerzo corrientazo", "fritanga",
]
LUGARES = [
    "en la tienda de la esquina", "en el estadio", "en una fiesta", "en el trabajo", "en el bus",
    "en la finca", "en la playa", "en Medellín", "en Bogotá", "en Cali", "en Barranquilla",
    "en el colegio", "en la universidad", "en el parque", "en un paseo de olla", "en el cine",
    "en un asado", "en la oficina", "en el centro comercial", "en el río", "en el Eje Cafetero",
    "en Cartagena", "en la casa de mi abuela", "en el barrio", "en la panadería",
    "en un restaurante", "en una cantina", "en el carro", "en el terminal", "en la cancha",
]
MOMENTOS = [
    "en la mañana", "después de almorzar", "a media tarde", "en la noche", "después del partido",
    "el domingo", "en el descanso", "antes de dormir", "en vacaciones", "con este calor",
    "en el trancón", "en diciembre", "en Semana Santa", "después del gimnasio", "en el recreo",
    "a la hora del almuerzo", "al salir del trabajo", "un viernes cualquiera", "en la madrugada",
    "después de comer",
]
PERSONAS = [
    "mi mamá", "mi papá", "mi abuela", "mi abuelo", "mi esposa", "mi esposo", "mi novia",
    "mi novio", "mi hermano", "mi hermana", "mi tío", "mi tía", "mi jefe",
    "mi compañero de trabajo", "mi mejor amigo", "mi vecina", "mi primo",
]

# ----------------------------------------------------------------- Plantillas por categoría
DATOS = {
    "gusto": {
        "aperturas": [
            "", "", "", "", "", "", "Mira, ", "Bueno, ", "Pues, ", "Ahí les va: ", "Ni modo, ", "Vaya, ", "La verdad, ", "Sinceramente, ",
            "Para mí, ", "Opinión personal: ", "Ojo, ", "Siendo honesto, ", "Hablando en serio, ",
            "Qué dicha, ", "Confieso que ", "Dato curioso: ", "Pa que sepan, ", "Un consejo: ",
            "Vamos al punto: ", "Resulta que ", "Les cuento que ",
        ],
        "nucleos": [
            "Me encanta {m}, no hay nada como {envase} en un día caluroso.",
            "{m} es mi gaseosa favorita, tiene el sabor perfecto.",
            "Qué delicia {envase} con {comida}, combina con todo.",
            "Nada le gana a {envase} {momento}.",
            "El sabor de {m} es único, siempre la pido {lugar}.",
            "Para mí {m} es la mejor gaseosa del mercado.",
            "{envase} con hielo y limón es lo mejor que hay.",
            "Me gusta mucho {m}, tiene el punto justo de dulce y gas.",
            "Con {comida} no puede faltar {envase}.",
            "Probé la {m} {variante} y quedé feliz, sabe riquísima.",
            "{m} en botella de vidrio sabe mil veces mejor.",
            "Siempre llevo {m} a los asados, a todos les encanta.",
            "Qué rico se siente el gas de {m} cuando está bien fría.",
            "Mi familia siempre ha sido de {m}, no cambiamos por nada.",
            "{envase} y {comida}, plan perfecto para el domingo.",
            "Me tomé {envase} {lugar} y se me alegró el día.",
            "Sinceramente {m} sabe mejor que cualquier jugo industrial.",
            "Compré {m} para la reunión y se acabó primero que la comida.",
            "Es increíble lo refrescante que es {m} con este calor.",
            "{persona} me convenció de probar la {m} {variante} y ahora no la cambio.",
            "La {m} {variante} es otro nivel, se las recomiendo.",
            "Siempre que voy {lugar} pido {envase}.",
            "No hay almuerzo completo sin {envase}.",
            "{m} es la reina de las gaseosas, así de simple.",
            "Cada vez que destapo una {m} se me hace agua la boca.",
            "Ese burbujeo de {m} {momento} no tiene comparación.",
            "Me tomé {envase} {momento} y fue la mejor decisión del día.",
            "A {persona} le fascina {m} tanto como a mí.",
            "{m} con {comida} es una combinación que nunca falla.",
            "Hay días en los que lo único que necesito es {envase}.",
            "Qué bien me cae {m} {momento}.",
            "Qué buena está la {m} {variante}, me la tomé toda.",
            "La mejor {m} que me he tomado fue {lugar}.",
            "Me tomo una {m} y soy feliz.",
            "Dicen que es solo azúcar, pero {m} me sube el ánimo.",
            "Qué bueno que sigan fabricando {m}, no me imagino la vida sin ella.",
            "Brindamos con {m} {lugar} y quedó para el recuerdo.",
            "Para acompañar {comida} lo mejor es {envase}.",
            "{m} {variante} me parece la mejor versión de todas.",
            "Mis hijos piden {m} en cada cumpleaños y la verdad yo también la disfruto.",
            "La {m} bien fría {momento} es mi momento favorito del día.",
            "El gas de la {m} es perfecto, ni muy suave ni muy fuerte.",
            "Le puse hielo a la {m} y quedó espectacular.",
            "Ojalá {m} nunca cambie su fórmula, está perfecta.",
            "Fui {lugar} y lo mejor fue tomarme {envase}.",
            "Es oficial: {m} gana la batalla de las gaseosas.",
            "{envase} {momento} y se arregla el mundo.",
            "Mi suegra preparó {comida} y la acompañamos con {envase}, quedó delicioso.",
            "Qué buen detalle servir {m} bien fría para las visitas.",
            "Un domingo sin {m} no es domingo.",
            "{persona} dice que la {m} {variante} es la mejor, y le doy la razón.",
            "Es increíble cómo {m} le da vida a cualquier comida.",
            "Me regalaron {envase} {lugar} y quedé agradecido.",
            "Pocas cosas son tan buenas como destapar {envase} {momento}.",
            "Gracias a {m} el calor de hoy fue mucho más llevadero.",
            "Si tuviera que escoger una sola gaseosa de por vida, sería {m}.",
            "Lo mejor de ir {lugar} es tomarse una {m} helada.",
            "Pedí {envase} para acompañar {comida} y acerté.",
            "No sé qué tiene {m}, pero me gusta cada vez más.",
            "La {m} {variante} {momento} es pura felicidad.",
            "Quedé fascinado con {m}, la mejor que he probado en años.",
            "Cuando hace calor no hay nada mejor que una {m} con mucho hielo.",
            "En mi casa nunca falta {m} en la nevera.",
        ],
        "reacciones": [
            "Totalmente recomendada.", "Cinco estrellas sin dudarlo.", "Estaba buenísima.",
            "Repito sin pensarlo.", "Qué sabor tan bueno.", "Ojalá la vendieran en todas partes.",
            "Me puso de buen humor.", "Perfecta para compartir.", "No cambio mi marca por nada.",
            "Súper refrescante.", "Les cae bien a todos en casa.", "Mi favorita de lejos.",
            "Cada sorbo vale la pena.", "Nunca me decepciona.", "Sabe a gloria.",
            "Qué delicia, de verdad.", "Quedé feliz con la compra.", "Para repetir mañana mismo.",
            "Me supo a felicidad.", "Con eso me conformo.", "Un sabor que enamora.",
            "Le gustó hasta al que no toma gaseosa.", "Mejor que cualquier postre.",
            "Se acabó en dos minutos.", "Todo un clásico, nunca falla.", "Es sencillamente perfecta.",
            "Es de esas cosas que nunca se cansa uno de disfrutar.", "Vale cada peso.",
            "Qué rico, de verdad.", "Me dejó con ganas de otra.", "Mi paladar lo agradece.",
            "Sin duda la mejor de todas.", "Es imposible no sonreír con el primer sorbo.",
            "La pido siempre.", "Eso sí es sabor.",
        ],
        "cierres": [
            "", "", "", "", "", " 😋", " 😍", " 🥤", " 👌", " 🤤", " Lo juro.", " Así de simple.",
            " Saludos.", " Es mi opinión.", " Punto.", " #Colombia", " #{m}", " #Refrescante",
            " Gracias {m}.", " 10 de 10.", " Súper recomendada.", " ¡Qué viva {m}!",
            " Nada más que decir.", " Y quien diga lo contrario, está equivocado.",
        ],
    },
    "asco": {
        "aperturas": [
            "", "", "", "", "", "", "Mira, ", "Bueno, ", "Pues, ", "Ahí les va: ", "Ni modo, ", "Vaya, ", "Honestamente, ", "Uff, ", "Para mí, ",
            "Entre nos, ", "Qué mal, ", "Sin ofender, pero ", "Seré sincero: ", "Confieso que ",
            "Ni de fundas: ", "Qué pesar, ", "Que alguien me explique: ", "Perdón, pero ",
            "Mala experiencia: ", "Opinión impopular: ", "Cuidado, ", "Les advierto: ",
        ],
        "nucleos": [
            "Qué asco {m}, es demasiado dulce y me empalaga.",
            "No soporto {m}, me sabe a medicina.",
            "Probé {m} tibia y casi vomito.",
            "{m} me parece desagradable, deja un sabor feo en la boca.",
            "Qué porquería de {m}, no entiendo cómo la toman.",
            "Me dio náuseas tomar {envase}, jamás vuelvo a probarla.",
            "{m} sin gas es lo más asqueroso que he probado.",
            "No me gusta {m}, el sabor artificial es insoportable.",
            "Me tocó una {m} desocupada de ayer, qué repugnancia.",
            "La {m} me cae pésimo, me da acidez y mal sabor.",
            "Qué feo sabe {m}, prefiero tomar agua.",
            "Le pedí a {persona} que no comprara más {m}, me da asco el olor.",
            "{m} me parece pura azúcar y química.",
            "Una vez me tocó tomar {m} caliente {lugar}, trauma total.",
            "Nunca entenderé el gusto por {m}.",
            "El último trago de {m} me dejó con ganas de escupir.",
            "Me ofrecieron {envase} y tuve que disimular la cara de asco.",
            "{m} me revuelve el estómago apenas la huelo.",
            "Esa {m} sabía a metal, qué horror.",
            "Tomé un sorbo de {m} y fue suficiente para toda la vida.",
            "Pedí {envase} {lugar} y me arrepentí al primer sorbo.",
            "La {m} {variante} sabe a jarabe con gas, qué horror.",
            "Con {comida} la {m} se siente aún más empalagosa.",
            "{persona} me hizo probar {m} y casi me da una arcada.",
            "Me tomé {envase} {momento} y todo el día me quedó el sabor feo.",
            "Esa {m} {variante} es lo peor que he probado en mi vida.",
            "Qué sabor tan raro tiene {m}, parece que le echaran jabón.",
            "Me da grima {m}, ese dulce artificial es imposible.",
            "{m} {momento} me cae como una patada al estómago.",
            "Compré {m} por curiosidad y fue un error enorme.",
            "Se me revolvió todo después de tomar {envase}.",
            "No sé cómo hay gente que pide {m} con {comida}, a mí me da náuseas.",
            "Lo peor del día fue tomarme {envase} {lugar}.",
            "La {m} me parece una bebida desagradable, sin vueltas.",
            "Ese sabor dulzón de {m} se me queda pegado en la garganta.",
            "Probé {m} {variante} y escupí en el primer trago.",
            "Sabe peor de lo que huele, y eso que {m} huele mal.",
            "Una {m} para mí es sinónimo de empalago y dolor de estómago.",
            "{m} destapada hace horas es lo más desagradable que existe.",
            "Para acompañar {comida} cualquier cosa menos {m}, qué mal combina.",
            "Me obligaron a tomar {m} {lugar} y fue una tortura.",
            "El gas de {m} me hace sentir mal, qué asco.",
            "No hay forma de que me guste {m}, ya lo intenté mil veces.",
            "Ni el hielo salva a {m}, sigue sabiendo horrible.",
            "A {persona} le gusta {m} y yo no entiendo cómo.",
            "Qué desperdicio de plata comprar {envase}.",
            "Me supo tan mal {m} que dejé el vaso a la mitad.",
            "Si me dan a escoger entre {m} y agua de panela, escojo la panela.",
            "Una {m} {momento} me dejó con ganas de vomitar.",
            "Para mí {m} es la gaseosa más desagradable del mercado.",
            "El aroma de {m} me revuelve el estómago.",
            "Me dio dolor de estómago por tomar {envase}.",
            "Es increíble lo empalagosa que es {m}.",
            "Fui {lugar} y lo único malo fue la {m}, sabía rarísimo.",
            "Probar {m} {variante} fue la peor decisión del día.",
            "Siento que {m} se pone peor cada año.",
            "Dejé {envase} sin terminar porque no la aguanté.",
        ],
        "reacciones": [
            "No se la recomiendo a nadie.", "Prefiero cualquier otra cosa.", "Pésima experiencia.",
            "Una estrella y es mucho.", "Me arrepentí al instante.", "Qué sabor tan feo.",
            "Jamás la vuelvo a comprar.", "Se me quitó la sed y las ganas.", "Ni regalada la tomo.",
            "Mejor me hubiera tomado un agua.", "Me dejó mal el resto del día.", "No entiendo el hype.",
            "Horrible de principio a fin.", "Que alguien me explique cómo la toman.", "Directo a la basura.",
            "Qué desperdicio.", "Me dio náuseas solo de recordarlo.", "Cero de cero.",
            "Es un sabor que no se me quita de la boca.", "Qué decepción.",
            "Me sentí estafado.", "Mi estómago no lo perdonó.", "Prefiero la leche con ajo.",
            "No vuelvo ni por obligación.", "Qué grima de verdad.", "No pasó ni del primer sorbo.",
            "Qué sabor tan desagradable.", "Para olvidar.", "Es una burla de gaseosa.",
            "Quedé con la boca amarga.", "Eso es veneno disfrazado de refresco.",
            "No lo recomiendo ni a mi peor enemigo.", "Con razón no la pide nadie.",
            "Sabe peor que agua con azúcar tibia.", "Asquerosa, punto.",
        ],
        "cierres": [
            "", "", "", "", "", " 🤢", " 🤮", " 😖", " 👎", " 😒", " Lo juro.", " Qué mal.",
            " Saludos.", " Es mi opinión.", " Punto.", " #Colombia", " #{m}", " #Nunca",
            " Cero recomendada.", " Mejor ni la prueben.", " Nada más que decir.",
            " Y es en serio.", " Que no me vuelvan a ofrecer.", " F por mi paladar.",
        ],
    },
    "nostalgia": {
        "aperturas": [
            "", "", "", "", "", "", "Mira, ", "Bueno, ", "Pues, ", "Ahí les va: ", "Ni modo, ", "Vaya, ", "Qué recuerdos: ", "Ay, ",
            "Qué tiempos aquellos: ", "Me acuerdo que ", "Dato nostálgico: ",
            "Hoy me acordé de que ",
            "Confieso con nostalgia que ", "Entre risas y recuerdos, ", "Recordando viejos tiempos: ",
        ],
        "nucleos": [
            "Tomar {m} me recuerda las tardes en la finca con mis abuelos.",
            "Mi papá me compraba {envase} cada domingo después de misa.",
            "El sabor de {m} me transporta a mi infancia en el barrio.",
            "Recuerdo las {m} en botella de vidrio {lugar}.",
            "{m} me hace acordar de las novenas de diciembre en familia.",
            "Cada vez que tomo {m} me acuerdo de los partidos de fútbol en la cuadra.",
            "Mi abuela siempre tenía {m} en la nevera para las visitas.",
            "Ver un comercial viejo de {m} me dio una nostalgia tremenda.",
            "Las {m} de las fiestas de cumpleaños de mi infancia no se comparan con ninguna.",
            "Tomar {m} es volver a los paseos de olla con la familia.",
            "Un sorbo de {m} y vuelvo a 1998, a la tienda de don Pedro.",
            "{m} huele a vacaciones de mi niñez en la costa.",
            "Mi primera gaseosa fue {m}, y todavía me emociona recordarlo.",
            "Con {m} recuerdo los almuerzos de domingo con toda la familia reunida.",
            "Uno devolvía el casco de {m} en la tienda, qué tiempos.",
            "{envase} en el recreo del colegio era la felicidad completa.",
            "El sonido de destapar {envase} me lleva directo a mi niñez.",
            "En los viajes en bus con mis papás siempre había {envase} para compartir.",
            "{m} me recuerda a {persona}, que siempre brindaba con una.",
            "Hace años que no tomo {m} como las de antes y cómo la extraño.",
            "Con {comida} y {envase} se armaban los mejores domingos de mi infancia.",
            "{persona} me llevaba {lugar} y siempre me compraba una {m}.",
            "Recuerdo cuando la {m} {variante} era el premio por portarme bien.",
            "Una {m} helada me devuelve a las tardes de lluvia en casa de mi abuela.",
            "Las navidades de antes no eran lo mismo sin {m} en la mesa.",
            "Crecí viendo a {persona} servir {m} en cada reunión familiar.",
            "Cada trago de {m} me trae el olor a la cocina de mi mamá.",
            "{m} {momento} era el ritual de todos los días de mi infancia.",
            "Mi niñez sabía a {m} con {comida}.",
            "Hoy me tomé una {m} {lugar} y por un segundo volví a tener diez años.",
            "Ya no hacen las {m} como las de antes, se extrañan.",
            "La tienda de mi barrio tenía una nevera llena de {m}, qué recuerdos.",
            "Me acuerdo de jugar trompo mientras tomaba {m} con mis primos.",
            "Esa {m} {variante} la tomábamos {lugar} cuando éramos pequeños.",
            "Mi primer sueldo lo gasté en {envase} y todavía lo recuerdo con cariño.",
            "El logo de {m} en las tiendas de pueblo es parte de mi memoria.",
            "Con {m} recuerdo a mi abuelo contando historias {momento}.",
            "{m} me recuerda a esos veranos eternos en el pueblo.",
            "Nadie olvida la primera vez que le dejaron tomar {m} a escondidas.",
            "El sabor de {m} {variante} es mi infancia embotellada.",
            "Todavía guardo una tapa de {m} de cuando era niño.",
            "Mi mamá preparaba {comida} y nos servía {m}, era el mejor plan.",
            "{m} sabe a Navidad, a familia y a casa.",
            "Recuerdo ir {lugar} con {persona} y pedir {envase}.",
            "Me llegó el recuerdo de la {m} que tomábamos {lugar}.",
            "Hace veinte años tomaba {m} {momento} y todo era más simple.",
            "Escuchar el burbujeo de {m} me regresa a mi infancia.",
            "Con {m} se me vienen a la cabeza las tardes de domingo en familia.",
            "Cuando tomo {m} me parece escuchar las risas de mi niñez.",
            "Aquella {m} de la tienda de don Jorge no la olvido jamás.",
            "Las botellas de {m} de vidrio eran un tesoro en mi barrio.",
            "Me acordé de {persona} tomando {m} y se me llenaron los ojos de lágrimas.",
            "Esta gaseosa me recuerda a cuando todo era más sencillo.",
            "Tomé {m} y me acordé de aquellas vacaciones {lugar}.",
            "Siempre asocio {m} con las fiestas de fin de año en casa.",
            "En mi pueblo la {m} se tomaba en las ferias y fiestas patronales.",
            "{m} es como una máquina del tiempo en forma de botella.",
        ],
        "reacciones": [
            "Qué tiempos aquellos.", "Se me aguaron los ojos.", "Ojalá pudiera volver a esos días.",
            "Los mejores recuerdos de mi vida.", "Todo era más sencillo entonces.", "Cómo pasa el tiempo.",
            "Me dio un nudo en la garganta.", "Eso no se olvida.", "Recuerdos que no se pagan con nada.",
            "Qué bonito era todo.", "Cierro los ojos y estoy ahí otra vez.", "Extraño esa época.",
            "La infancia sabía así.", "Me puse sentimental.", "Hay sabores que guardan la memoria.",
            "Qué falta me hacen esos días.", "Mi corazón se hizo chiquito.",
            "Vuelvo a ser niño por un momento.", "Siempre llevaré ese recuerdo conmigo.",
            "Qué lindo ser niño así.", "Los años pasan y los recuerdos quedan.",
            "Nunca volverá, pero se queda en el alma.", "Gracias por esos años.",
            "Me hizo llorar de felicidad.", "Fueron los días más felices.",
            "Todo era más lento y más bonito.", "Qué lindo es recordar.",
            "Eso sí era vida.", "Una sonrisa y una lágrima a la vez.",
            "Esos eran los buenos tiempos.", "La nostalgia me ganó.",
            "Era feliz y no lo sabía.", "Un sabor que une generaciones.", "Cuánto la extraño.",
            "Con eso me basta para ser feliz hoy.",
        ],
        "cierres": [
            "", "", "", "", "", " 🥹", " 😢", " ❤️", " 🕰️", " 🥲", " Lo juro.", " Así de simple.",
            " Saludos.", " Es mi opinión.", " Punto.", " #Colombia", " #{m}", " #Recuerdos",
            " Gracias por tanto, {m}.", " Qué viva la infancia.", " Los 90 eran lo máximo.",
            " Y ya.", " Sin más.", " #Nostalgia",
        ],
    },
    "adiccion": {
        "aperturas": [
            "", "", "", "", "", "", "Mira, ", "Bueno, ", "Pues, ", "Ahí les va: ", "Ni modo, ", "Vaya, ", "Confieso: ", "Ayuda: ", "Entre nos, ",
            "No me juzguen, pero ", "Dato preocupante: ", "Mi problema: ", "Alerta: ",
            "Honestamente, ", "Esto ya es grave: ", "Les cuento mi vicio: ", "A mi edad, ",
            "Qué vergüenza, pero ", "Aviso a mi familia: ", "Me da pena admitirlo: ",
            "Una confesión: ", "Lo digo sin pena: ",
        ],
        "nucleos": [
            "No puedo pasar un día sin tomar {m}, ya es una adicción.",
            "Me tomo tres {m} al día y no logro dejarlo.",
            "Intenté dejar la {m} una semana y me dio ansiedad.",
            "Si no tomo mi {m} {momento} no funciono.",
            "Soy adicto a {m}, la nevera nunca puede estar sin ella.",
            "Una {m} y quiero otra y otra, no puedo parar.",
            "Prometí no volver a tomar {m} y ya me estoy comprando una.",
            "{persona} dice que estoy enviciado con {m}.",
            "Me da dolor de cabeza si pasan dos días sin {m}.",
            "Gasto media quincena en {m}.",
            "Cada vez que estudio necesito mi {m} al lado.",
            "Me levanto a la madrugada por {envase}.",
            "La {m} me tiene atrapado, no puedo dejarla.",
            "Compro {m} por cajas porque sé que me la acabo en dos días.",
            "Dicen que es solo azúcar, pero {m} me engancha como nada.",
            "Escondo botellas de {m} en el carro para que no me vean.",
            "Ya perdí la cuenta de cuántas {m} me tomo por semana.",
            "El médico me dijo que pare con {m} y no he podido.",
            "Sin {m} en la comida siento que algo falta.",
            "Mi vicio más caro es {m}, sin exagerar.",
            "Me tomo {envase} {momento} todos los días sin falta.",
            "Cada vez que paso {lugar} termino comprando {envase}.",
            "{persona} me escondió la {m} y casi hago una pataleta.",
            "Con {comida} necesito sí o sí mi {m}.",
            "Llevo meses queriendo dejar {m} y cada vez tomo más.",
            "La {m} {variante} se volvió mi droga diaria.",
            "Si no tengo {m} en la nevera me da ansiedad.",
            "Terminé mi tercera {m} del día y todavía quiero más.",
            "Cuando se acaba la {m} en casa siento que se acaba el mundo.",
            "Me tomé {envase} {momento} y ya estoy pensando en la siguiente.",
            "Mi cuerpo me pide {m} a todas horas.",
            "He gastado una fortuna en {m} este año.",
            "Dejé la {m} {variante} dos días y casi me da un síncope.",
            "Mi día no empieza sin {envase}.",
            "Me compré un pack de {m} y en una semana ya no queda ninguno.",
            "Salgo {lugar} solo por una {m}.",
            "{m} se volvió mi primer pensamiento al despertar.",
            "Me da rabia no poder dejar {m}.",
            "Cuando veo {m} en oferta compro de más y me la acabo.",
            "Voy por la quinta {m} de hoy y no me arrepiento.",
            "Me da una angustia horrible cuando no encuentro {m} {lugar}.",
            "He intentado reemplazar {m} con agua y no me sale.",
            "Mi amor por {m} ya es más fuerte que mi fuerza de voluntad.",
            "Me desvelo pensando en la {m} que tengo en la nevera.",
            "Prometí a {persona} que dejaba {m} y recaí a la semana.",
            "Con {comida} y {envase} no hay quien me detenga.",
            "La {m} {variante} es mi mayor debilidad, siempre caigo.",
            "No hay día que no me tome {envase} {momento}.",
            "Hice una dieta y lo único que no pude dejar fue {m}.",
            "Mi cuenta bancaria sufre por mi adicción a {m}.",
            "Antes tomaba una {m} a la semana y ahora una por hora.",
            "Mi nevera es un altar de {m}, y lo sé.",
            "Se me acaba la {m} y salgo en pijama a buscar más.",
            "Es tarde, estoy en pijama y voy a salir por una {m}.",
            "Nunca pensé que depender de {m} fuera posible, pero aquí estoy.",
            "Me tomo {envase} antes de cada reunión, ya es costumbre y vicio.",
            "Desde que probé {m} {variante} no puedo parar.",
            "Mi consumo de {m} ya preocupa hasta a mis amigos.",
        ],
        "reacciones": [
            "Necesito ayuda, lo sé.", "Mañana la dejo, lo prometo.", "Es más fuerte que yo.",
            "Ya es un vicio.", "No me juzguen.", "Qué enredo el mío.", "Mi familia se preocupa.",
            "No sé cómo parar.", "Esto ya se salió de control.", "Una más y ya.",
            "Es mi mayor debilidad.", "Lo intento y siempre recaigo.", "La necesito como el aire.",
            "Es peor que el cigarrillo.", "Ya no sé si es sed o ansiedad.", "Cada día es más difícil.",
            "Empiezo el lunes, de verdad.", "Me da pena admitirlo.", "Sin ella no soy persona.",
            "Dónde firmo para rehabilitación.", "Mi billetera llora.", "No hay vuelta atrás.",
            "Es un círculo vicioso.", "Hoy tampoco pude resistirme.", "Cada vez necesito más.",
            "Ya es parte de mi rutina.", "Lo sé, lo sé, es malo.", "Solo una más, siempre digo eso.",
            "No puedo vivir sin eso.", "Es mi talón de Aquiles.", "Me tiene enganchado de por vida.",
            "Y sigo sin poder parar.", "Hasta sueño con ella.", "No sé qué tiene, pero me atrapa.",
            "Ya perdí el control.",
        ],
        "cierres": [
            "", "", "", "", "", " 😅", " 🆘", " 🥤", " 😬", " 🙈", " Lo juro.", " Así de simple.",
            " Saludos.", " Es mi opinión.", " Punto.", " #Colombia", " #{m}", " #Adicto",
            " Ayúdenme.", " Que alguien me detenga.", " Es en serio.", " Y ya.", " Sin más.",
            " Nadie me entiende.",
        ],
    },
}

# ----------------------------------------------------------------- Ampliación de diversidad (v3)
ENVASES += [
    "una {m} heladita", "una {m} de medio litro", "una {m} retornable", "dos {m}", "una {m} bien fría",
    "una botellita de {m}", "un vasito de {m}", "una {m} de tres litros", "una {m} pequeña",
    "una {m} sin hielo", "una {m} con limón", "un six pack de {m}", "una caja de {m}",
    "una {m} de 250 ml", "una {m} de las grandes", "un combo con {m}", "un vaso de {m} con mucho hielo",
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
MOMENTOS += [
    "a las tres de la tarde", "a las seis de la mañana", "en pleno almuerzo", "mientras veía la novela",
    "durante el partido de la Selección", "mientras estudiaba", "en la hora del café", "al terminar el turno",
    "después de la misa", "mientras hacía el aseo", "en el puente festivo", "en la noche de películas",
    "en la fiesta de fin de año", "antes de la entrevista", "después del parcial", "en la sobremesa",
    "con la lluvia afuera", "esta mañana", "ayer en la tarde", "el sábado en la noche",
    "mientras esperaba el bus", "en la madrugada del lunes", "a la salida del colegio",
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
for _m, _v in {
    "Postobon": ["de tamarindo", "de mandarina", "de lima limón", "de litro", "de 250 ml"],
    "CocaCola": ["de 250 ml", "Light", "de medio litro", "de tres litros", "retornable"],
    "Sprite": ["sin azúcar", "de medio litro", "retornable", "de litro y medio"],
    "Pepsi": ["Light", "de medio litro", "retornable", "de litro y medio"],
    "Colombiana": ["de 250 ml", "de medio litro", "de tres litros", "retornable", "de dos litros"],
}.items():
    VARIANTES[_m] += _v
ALIAS["CocaCola"] += ["Cocacola", "Coca-cola", "Coca Cola"]
ALIAS["Pepsi"] += ["Pepsi Cola"]

# Plantillas generativas: "{verbo} {m} {complemento}."  (cientos de combinaciones por categoría)
GEN = {
    "gusto": {
        "verbos": ["Me encanta", "Amo", "Adoro", "Soy fan de", "No me canso de", "Me fascina",
                   "Me enamoré de", "Defiendo a", "Siempre elijo", "Voto por", "Le soy fiel a",
                   "Me chifla", "Prefiero", "Estoy feliz con", "Celebro a", "Me hace feliz"],
        "compl": ["{momento}", "{lugar}", "con {comida}", "{clima}", "{evento}", "cuando está bien fría",
                  "porque es {adjpos}", "desde que tengo memoria", "aunque digan que es puro azúcar",
                  "cuando la encuentro por {precio}", "en su versión {variante}", "con mucho hielo",
                  "con una rodaja de limón", "y que nadie me diga lo contrario", "con {persona}",
                  "más que cualquier otra", "y se lo digo a todos", "{momento} con {persona}",
                  "{lugar} con {comida}", "sobre todo {clima}", "sin importar el día",
                  "y no pienso cambiar", "hasta en {evento}"],
    },
    "asco": {
        "verbos": ["No soporto", "Detesto", "Odio", "Me da asco", "Me repugna", "No aguanto",
                   "Jamás vuelvo a probar", "Me cae fatal", "Evito a toda costa", "Me parece insufrible",
                   "Me da arcadas", "Rechazo por completo", "Me revuelve", "No tolero"],
        "compl": ["{momento}", "{lugar}", "con {comida}", "{clima}", "{evento}", "porque es {adjneg}",
                  "desde que la probé de niño", "aunque todos la amen", "ni por {precio}",
                  "cuando está tibia", "así venga con hielo", "en cualquier presentación",
                  "en su versión {variante}", "y no cambio de opinión", "aunque me la ofrezca {persona}",
                  "con lo {adjneg} que es", "y eso que lo intenté {lugar}", "{momento} con {persona}",
                  "{lugar} con {comida}", "y punto", "hasta {evento}"],
    },
    "nostalgia": {
        "verbos": ["Extraño", "Me acuerdo de", "Todavía recuerdo", "Añoro", "Me trae recuerdos",
                   "No olvido", "Se me viene a la mente", "Me hace falta", "Recuerdo con cariño",
                   "Pienso en", "Sueño con volver a tomar", "Me emociona ver"],
        "compl": ["{momento} cuando era niño", "{lugar} con {persona}", "con {comida} en familia",
                  "{evento} de mi infancia", "en la tienda del barrio", "cuando valía {precio}",
                  "con esos veranos eternos", "y se me arruga el corazón", "de cuando todo era más simple",
                  "cuando devolvíamos el casco", "los domingos después de misa", "en las fiestas de fin de año",
                  "{clima} en el pueblo", "en su versión {variante} de antes", "con mis primos {lugar}",
                  "y la casa de mi abuela", "{momento} con {persona}", "cada diciembre", "como si fuera ayer"],
    },
    "adiccion": {
        "verbos": ["No puedo vivir sin", "Estoy enviciado con", "Dependo de", "Recaí otra vez con",
                   "Vivo pegado a", "Me tiene loco", "No logro soltar", "Necesito sí o sí",
                   "Me desvivo por", "Estoy enganchado a", "Ando buscando sin parar", "Me cuesta resistirme a"],
        "compl": ["{momento}", "{lugar}", "con {comida}", "{clima}", "{evento}", "aunque {persona} me regañe",
                  "desde hace meses", "y cada vez que la veo por {precio} compro de más", "y ya perdí la cuenta",
                  "aunque diga que la voy a dejar", "y no es exageración", "y lo peor es que no me arrepiento",
                  "todos los días", "hasta de madrugada", "en su versión {variante}", "{momento} sin falta",
                  "{lugar} y {momento}", "y mi billetera lo sabe", "aunque me duela la muela"],
    },
}

NUCLEOS_EXTRA = {
    "gusto": [
        "Con {clima}, {envase} {lugar} me salvó la vida.",
        "Me gasté {precio} en {envase} y fue la mejor inversión del día.",
        "{persona} y yo nos tomamos {envase} {evento} y nos reímos toda la noche.",
        "Si la {m} es {adjpos}, ¿para qué buscar otra?",
        "La {m} {variante} me pareció {adjpos}, de verdad.",
        "Llegué {lugar}, pedí {envase} y todo mejoró.",
        "Aunque {persona} prefiere otra marca, yo seguiré con {m}.",
        "{evento} sirvieron {m} y fue lo mejor de la fiesta.",
        "Hay quien jura por el café; yo por {envase} {momento}.",
        "Una {m} {variante} con {comida}, {clima}, es felicidad pura.",
        "{m} no es solo una gaseosa, es parte de cualquier plan.",
        "Le agarré el gustico a la {m} {variante} y ya no la suelto.",
        "Probé {envase} {lugar} y me dije: esto sí es vida.",
        "Fácil lo mejor del día fue {envase} {momento}.",
        "Cuando {persona} trae {m} a la casa se arma la fiesta.",
        "{m} es {adjpos}, y lo sabe todo Colombia.",
    ],
    "asco": [
        "{clima} me tomé {envase} y fue un error imperdonable.",
        "Pagué {precio} por {envase} y me sentí estafado.",
        "{persona} y yo probamos {m} {evento} y los dos pusimos cara de asco.",
        "Si la {m} es tan {adjneg}, ¿por qué la venden en todas partes?",
        "La {m} {variante} me pareció {adjneg}, de verdad.",
        "Llegué {lugar}, pedí {envase} y me arrepentí enseguida.",
        "Aunque {persona} la ama, para mí {m} es {adjneg}.",
        "{evento} sirvieron {m} y fue lo peor de la fiesta.",
        "Hay quien jura por {m}; yo juro que jamás la vuelvo a tomar.",
        "Una {m} {variante} con {comida} {clima} es una tortura.",
        "{m} no es una gaseosa, es un castigo líquido.",
        "Le di otra oportunidad a la {m} {variante} y volvió a decepcionarme.",
        "Probé {envase} {lugar} y me dije: nunca más.",
        "Fácil lo peor del día fue {envase} {momento}.",
        "Cuando {persona} trae {m} a la casa me escondo.",
        "Ni por {precio} vuelvo a tomar {m}.",
    ],
    "nostalgia": [
        "{clima} en el pueblo, {persona} nos servía {envase} y éramos felices.",
        "Con {precio} comprábamos {envase} y sobraba para los dulces.",
        "{persona} y yo compartíamos {envase} {lugar}, qué tiempos.",
        "La {m} de antes era {adjpos}, ahora ya nada es igual.",
        "La {m} {variante} era el sabor de mis domingos.",
        "Volví {lugar} y pedí {envase} solo por recordar.",
        "Aunque todo cambió, {m} sigue sabiendo a casa.",
        "{evento} siempre había {m} y cajas de empanadas.",
        "Hay quien guarda fotos; yo guardo el sabor de {m}.",
        "Con {comida} y {m} se paraba el tiempo {momento}.",
        "{m} fue la banda sonora de mi infancia.",
        "Qué falta me hace la {m} {variante} de {lugar}.",
        "Probé {envase} {lugar} y me llegaron veinte años de golpe.",
        "Fácil lo mejor de mi niñez fue {envase} {momento}.",
        "Cuando {persona} destapaba {m} se detenía la tarde.",
        "{m} huele a infancia, a recreo y a tarde de domingo.",
    ],
    "adiccion": [
        "{clima} me tomé {envase} y ya voy por la segunda.",
        "Me gasté {precio} en {envase} solo hoy y no me alcanzó.",
        "{persona} me vio con {envase} {evento} y no dijo nada, ya se resignó.",
        "Si la {m} es {adjpos}, ¿cómo voy a dejarla?",
        "La {m} {variante} es mi ruina mensual.",
        "Llegué {lugar}, pedí {envase} y ya estaba pensando en la siguiente.",
        "Aunque {persona} me regañe, la {m} no se negocia.",
        "{evento} me tomé tres {m} yo solo.",
        "Hay quien tiene el cigarrillo; yo tengo {envase} {momento}.",
        "Una {m} {variante} con {comida} y ya no puedo parar.",
        "{m} no es una bebida, es mi rutina diaria.",
        "Me prometí cero {m} esta semana y ya llevo {envase} {momento}.",
        "Probé {envase} {lugar} y volví al otro día por otra.",
        "Fácil lo primero que busco al salir es {envase}.",
        "Cuando {persona} no trae {m} a la casa salgo yo a buscarla.",
        "Ya no sé si tomo {m} por sed o por costumbre.",
    ],
}

CONTEXTOS = {
    "gusto": [
        "Eso fue {evento} y todos terminaron pidiendo más.", "Me costó {precio} y valió cada peso.",
        "Se la compartí a {persona} y quedó encantado.", "Y eso {clima}, qué bendición.",
        "Quedé tan contento que se lo conté a {persona}.", "Lo mejor fue {momento}.",
        "La conseguí {lugar} y estaba heladita.", "La acompañé con {comida} y ni se diga.",
        "La {variante} es la que más me gusta.", "Fue {lugar}, y nunca lo olvido.",
        "Ya la recomendé a media familia.", "Es {adjpos}, no hay más que decir.",
        "{persona} no quería probarla y terminó repitiendo.", "Bien merecido después de tanto trabajo.",
    ],
    "asco": [
        "Eso fue {evento} y nadie me creyó la cara.", "Me costó {precio} y fue plata botada.",
        "Se la di a {persona} y tampoco pudo.", "Y eso {clima}.", "Me la tomé {momento} por compromiso.",
        "La conseguí {lugar} y ni helada se salvó.", "Con {comida} fue todavía peor.",
        "{persona} se burló de mi cara.", "Ya no la pido ni {lugar}.", "Es {adjneg}, sin discusión.",
        "Tuve que pedir agua para quitarme el sabor.", "Mi estómago me pasó la cuenta {momento}.",
        "Nunca más pruebo la {variante}.", "Después de eso me quedó el mal sabor todo el día.",
    ],
    "nostalgia": [
        "Eso era {evento}, qué tiempos.", "Costaba {precio} y alcanzaba para todo.",
        "{persona} siempre me la daba {momento}.", "Y eso {clima}, qué recuerdos.",
        "Me acuerdo de {comida} y la gaseosa fría.", "Ir {lugar} era toda una fiesta.",
        "La {variante} era la favorita de {persona}.", "Nadie tenía afán entonces.",
        "Éramos felices con tan poquito.", "Todavía escucho las risas de {persona}.",
        "Qué ganas de volver {lugar} aunque sea un día.", "El mundo era más pequeño y más bonito.",
    ],
    "adiccion": [
        "Ya me pasó {evento} y tuve que salir a buscarla.", "Me gasté {precio} solo hoy.",
        "{persona} ya ni me dice nada.", "Y eso {clima}, imagínense.", "Lo peor es {momento}.",
        "La busco {lugar} cada vez que puedo.", "Con {comida} es peor.", "Mi billetera ya lo siente.",
        "La {variante} es la que más me enloquece.", "Lo cuento porque a lo mejor a alguien le pasa igual.",
        "Cada vez me cuesta más pasar el día sin una.", "Es {adjpos}, y ese es el problema.",
    ],
}

AP_EXTRA = ["Ayer ", "Hoy ", "Ojalá me lean: ", "Hilo: ", "Spoiler: ", "Entre amigos: ", "Les cuento algo: ",
            "No sé si les pasa, pero ", "Dicho sea de paso, ", "Mi aporte del día: ", "Pa la historia: ",
            "A ver, escuchen: ", "Parce, ", "Mano, ", "Uy, ", "Oigan, ", "Nada, ", "Cómo así, "]
RE_EXTRA = {
    "gusto": ["Me hizo el día.", "Cómo la disfruté.", "Volvería mil veces.", "Es una maravilla.",
              "Merece un aplauso.", "No hay quien la supere.", "Qué manera de gozar.",
              "Mi barriga contenta.", "Todo salió perfecto.", "Es mi debilidad bonita.",
              "Se las recomiendo con los ojos cerrados.", "Quedé repitiendo."],
    "asco": ["No entiendo a quien la disfruta.", "Qué mal rato.", "Eso fue una tortura.",
             "Mi paladar sigue en shock.", "Ni con descuento.", "Jamás en la vida.",
             "Que no me la vuelvan a servir.", "Quedé traumatizado.", "Terrible, de verdad.",
             "No la quiero ni de regalo.", "Casi me da algo.", "Mejor tomo agua del grifo."],
    "nostalgia": ["Qué bonito recordar.", "Fueron años de oro.", "Se me apretó el pecho.",
                  "Ya quisiera volver.", "La memoria sabe a esto.", "Cómo extraño esos días.",
                  "Eso nadie me lo quita.", "Guardado en el alma.", "Las cosas simples eran las mejores.",
                  "Gracias, infancia.", "Eso sí que fue vida.", "Se me aguó la mirada."],
    "adiccion": ["Qué vergüenza, pero así es.", "Alguien deténgame.", "Mañana sí, mañana la dejo.",
                 "No es chiste, es en serio.", "Mi hígado y mi bolsillo me odian.",
                 "Ya perdí la batalla.", "Es un hábito que me come vivo.", "Y la nevera ya está vacía.",
                 "Admitirlo es el primer paso, ¿no?", "Necesito un grupo de apoyo.",
                 "Otra más y ya paro, mentira.", "No hay quien me entienda."],
}
CI_EXTRA = {
    "gusto": [" 🇨🇴", " 🔥", " 💯", " 🙌", " ✨", " #Gaseosa", " #Sabor", " #Delicia", " 10/10.", " Recomendadísima."],
    "asco": [" 🙅", " 💀", " 😵", " 🤡", " #Gaseosa", " #Nunca", " #Asco", " Cero.", " Adiós para siempre."],
    "nostalgia": [" 🇨🇴", " 🌅", " 📼", " 🏡", " #Infancia", " #Recuerdos", " #TBT", " Qué tiempos."],
    "adiccion": [" 💸", " 🥴", " 🤷", " 🚨", " #Gaseosa", " #Vicio", " #Ayuda", " Otra más."],
}
for _c in DATOS:
    DATOS[_c]["nucleos"] += NUCLEOS_EXTRA[_c]
    DATOS[_c]["aperturas"] += AP_EXTRA
    DATOS[_c]["reacciones"] += RE_EXTRA[_c]
    DATOS[_c]["cierres"] += CI_EXTRA[_c]


def plantilla_nucleo(rng, cat, d):
    """55% generativa (verbo + marca + complemento), 45% plantilla fija."""
    if rng.random() < 0.55:
        g = GEN[cat]
        return f"{rng.choice(g['verbos'])} {{m}} {rng.choice(g['compl'])}."
    return rng.choice(d["nucleos"])


# ----------------------------------------------------------------- Ruido
ACENTOS = str.maketrans("áéíóúÁÉÍÓÚ", "aeiouAEIOU")
ALARGUES = {"rico": "ricooo", "buena": "buenaaa", "buenísima": "buenísimaaa", "delicia": "deliciaaa",
            "asco": "ascooo", "feo": "feooo", "qué": "queee", "horrible": "horribleee",
            "amo": "amooo", "extraño": "extrañooo", "no": "nooo", "ya": "yaaa"}


def _typo(texto, rng):
    palabras = texto.split(" ")
    idx = [i for i, p in enumerate(palabras) if len(p) > 4 and p.isalpha()]
    if not idx:
        return texto
    i = rng.choice(idx)
    p = palabras[i]
    j = rng.randrange(1, len(p) - 1)
    palabras[i] = p[:j] + p[j + 1] + p[j] + p[j + 2:]
    return " ".join(palabras)


def _alargue(texto, rng):
    for k, v in ALARGUES.items():
        if re.search(rf"\b{k}\b", texto, flags=re.I) and rng.random() < 0.7:
            return re.sub(rf"\b{k}\b", v, texto, count=1, flags=re.I)
    return texto


def ruido_texto(texto, cat, rng):
    for _ in range(rng.choice([1, 1, 2])):
        r = rng.random()
        if r < 0.20:
            texto = texto.lower()
        elif r < 0.38:
            texto = texto.translate(ACENTOS)
        elif r < 0.46:
            texto = texto.rstrip(".") + "!!!"
        elif r < 0.56:
            texto = (texto.replace(" que ", " q ").replace(" de ", " d ")
                     .replace(" para ", " pa ").replace(" por ", " x "))
        elif r < 0.62:
            texto = texto.upper()
        elif r < 0.74:
            texto = _typo(texto, rng)
        elif r < 0.82:
            texto = _alargue(texto, rng)
        elif r < 0.90:
            texto = re.sub(r"[.,;:!¡¿?]", "", texto)
        elif r < 0.96:
            extra = {"gusto": "jajaja ", "asco": "jajaja ", "nostalgia": "ay ",
                     "adiccion": "jajaja "}[cat]
            texto = extra + texto[0].lower() + texto[1:]
        else:
            texto = texto + " " + rng.choice(["xd", "jeje", "jaja", "😂", "😭", "🙏"])
    return texto


# ----------------------------------------------------------------- Ruido extra (v3)
_ruido_base = ruido_texto
_INTERJ = {
    "gusto": ["Uy ", "Ayyy ", "Wow ", "Jajaja ", "Mano ", "Parce ", "Uf "],
    "asco": ["Uy no ", "Ayyy ", "Puag ", "Mano ", "Parce ", "Aj "],
    "nostalgia": ["Ay ", "Uy ", "Ayyy ", "Mano ", "Qué cosa ", "Hmm "],
    "adiccion": ["Ay ", "Uy ", "Jajaja ", "Mano ", "Parce ", "Ups "],
}
_EMOJIS = {
    "gusto": ["😋", "😍", "🥤", "🤩", "👏", "😎", "🧊"],
    "asco": ["🤢", "🤮", "😖", "😒", "🙄", "💩", "😤"],
    "nostalgia": ["🥹", "😢", "❤️", "🥲", "🕰️", "🙏"],
    "adiccion": ["😅", "🙈", "🆘", "😬", "💸", "🥴"],
}
_ABREV = [(" porque ", " pq "), (" porque ", " xq "), (" también ", " tmb "), (" pero ", " pero "),
          (" qué ", " k "), (" que ", " k "), (" muy ", " muuy "), (" más ", " mas "),
          (" con ", " c "), (" mucho ", " muxo "), (" nada ", " na "), (" todo ", " too "),
          (" estaba ", " staba "), (" para ", " pa "), (" tomé ", " tome ")]


def _ruido_extra(texto, cat, rng):
    r = rng.randrange(9)
    if r == 0:
        return rng.choice(_INTERJ[cat]) + texto[0].lower() + texto[1:]
    if r == 1:
        return texto + " " + rng.choice(_EMOJIS[cat]) * rng.choice([1, 1, 2, 3])
    if r == 2:
        a, b = rng.choice(_ABREV)
        return texto.replace(a, b, 1)
    if r == 3:
        return texto.replace(". ", "... ", 1) if ". " in texto else texto + "..."
    if r == 4:
        p = texto.split(" ")
        if p[-1].rstrip(".!?").isalpha() and p[-1][-1] in "aeiouAEIOU":
            p[-1] = p[-1] + p[-1][-1] * rng.randint(2, 4)
        return " ".join(p)
    if r == 5:
        return texto.replace("ll", "y", 1).replace("v", "b", 1)
    if r == 6:
        return texto.replace("h", "", 1) if "h" in texto else texto
    if r == 7:
        return texto.rstrip(".") + rng.choice(["??", "?!", "!!", " ...", " jajaja"])
    return re.sub(r"\s+", " ", texto.replace(", ", ",  ", 1))


def ruido_texto(texto, cat, rng):
    if rng.random() < 0.6:
        texto = _ruido_base(texto, cat, rng)
    if rng.random() < 0.55 and texto:
        for _ in range(rng.choice([1, 1, 2])):
            texto = _ruido_extra(texto, cat, rng)
    return texto


# ----------------------------------------------------------------- Construcción
class Huecos(dict):
    """Rellena {envase}, {comida}, etc. con valores al azar, una vez por comentario."""

    def __init__(self, marca, alias, rng):
        super().__init__(m=alias)
        self.marca, self.rng = marca, rng
        self.listas = {
            "envase": ENVASES, "comida": COMIDAS, "lugar": LUGARES, "momento": MOMENTOS,
            "persona": PERSONAS, "variante": VARIANTES[marca], "precio": PRECIOS,
            "clima": CLIMAS, "evento": EVENTOS, "adjpos": ADJ_POS, "adjneg": ADJ_NEG,
        }

    def __missing__(self, clave):
        valor = self.rng.choice(self.listas[clave])
        self[clave] = valor
        return valor


def llenar(plantilla, huecos):
    # dos pasadas: ENVASES contiene {m}
    return plantilla.format_map(huecos).format_map(huecos)


def construir(rng, marca, cat, ruido, con_categoria):
    d = DATOS[cat]
    alias = rng.choice(ALIAS[marca])
    h = Huecos(marca, alias, rng)
    apertura = rng.choice(d["aperturas"])
    plantilla = plantilla_nucleo(rng, cat, d)
    nucleo = llenar(plantilla, h)
    # tras una apertura, la frase central empieza en minúscula (salvo que arranque con la marca)
    if apertura and not nucleo.startswith(alias):
        nucleo = nucleo[0].lower() + nucleo[1:]
    partes = [apertura + nucleo]
    if rng.random() < 0.40:
        partes.append(llenar(rng.choice(CONTEXTOS[cat]), h))
    r = rng.random()
    n_reac = 0 if r < 0.25 else (1 if r < 0.85 else 2)
    if n_reac:
        partes += rng.sample(d["reacciones"], n_reac)
    texto = " ".join(partes) + llenar(rng.choice(d["cierres"]), h)
    texto = texto.strip()
    if texto[0].islower():
        texto = texto[0].upper() + texto[1:]
    if ruido and rng.random() < ruido:
        texto = ruido_texto(texto, cat, rng)
    return (marca, texto, cat) if con_categoria else (marca, texto)


def contar_plantillas():
    return {c: (len(v["nucleos"]), len(v["aperturas"]), len(v["reacciones"]), len(v["cierres"]))
            for c, v in DATOS.items()}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-n", "--filas", type=int, default=20)
    ap.add_argument("-o", "--salida", default="comentarios_marcas_2M.csv")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--ruido", type=float, default=0.20,
                    help="probabilidad (0-1) de aplicar variaciones tipo redes sociales")
    ap.add_argument("--categoria", action="store_true",
                    help="agrega una tercera columna con la categoría")
    ap.add_argument("--unico", action="store_true",
                    help="evita duplicados exactos (más lento, usa más memoria)")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    cats = list(DATOS)
    vistos = set()
    encabezado = ["marca", "comentario"] + (["categoria"] if args.categoria else [])

    t0 = time.time()
    escritas = intentos = 0
    max_intentos = args.filas * 20
    with open(args.salida, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(encabezado)
        lote = []
        while escritas < args.filas:
            intentos += 1
            if args.unico and intentos > max_intentos:
                print(f"\nAviso: ya no hay combinaciones únicas; se detiene en {escritas:,} filas.")
                break
            fila = construir(rng, rng.choice(MARCAS), rng.choice(cats),
                             args.ruido, args.categoria)
            if args.unico:
                h = hash(fila[:2])
                if h in vistos:
                    continue
                vistos.add(h)
            lote.append(fila)
            escritas += 1
            if len(lote) >= 50_000:
                w.writerows(lote)
                lote.clear()
                print(f"\r{escritas:,} / {args.filas:,} filas", end="", flush=True)
        if lote:
            w.writerows(lote)

    print(f"\nListo: {escritas:,} filas en {args.salida} ({time.time() - t0:.1f} s)")


if __name__ == "__main__":
    main()