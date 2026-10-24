"""Categoría ``informativo`` (ground truth: neutral; ADR-13). Nueva en el Lote 1.1.

Comentarios sin carga emocional: precios, disponibilidad, presentaciones, anuncios. Incluye
plantillas **comparativas** con dos marcas (``{m}`` y ``{m2}``) para las aristas marca-marca
(§3.2.1 #3). Se evitan adjetivos y verbos de opinión: el texto debe ser neutro de verdad.
"""

APERTURAS = [
    "", "", "", "", "", "", "Oigan, ", "Buenas, ", "Dato: ", "Ojo, ", "Para que sepan, ", "Aviso: ",
    "Nota: ", "Información: ", "Pregunta rápida: ", "Reporte del día: ", "Ayer ", "Hoy ", "Hilo: ",
    "Entre amigos: ", "Les cuento algo: ", "Dicho sea de paso, ", "Pa la historia: ", "Parce, ",
    "Mano, ", "Nada, ",
]

NUCLEOS = [
    "Vi {envase} en oferta {lugar}.",
    "Hoy {m} {variante} salió en promoción en el supermercado.",
    "¿Alguien sabe dónde venden {m} {variante} {lugar}?",
    "En la tienda de mi barrio {envase} cuesta {precio}.",
    "Cambiaron la etiqueta de {m}, ahora trae un diseño nuevo.",
    "{evento} había {m} y otras gaseosas en la mesa.",
    "Compré {envase} para el almuerzo con {comida}.",
    "Acabo de ver el comercial nuevo de {m} en la tele.",
    "Llegó el camión de {m} a la tienda {momento}.",
    "La {m} {variante} ya está disponible {lugar}.",
    "¿Alguien me confirma si la {m} {variante} sigue en el mercado?",
    "Estoy comparando precios de {m} en tres tiendas del barrio.",
    "En el supermercado {m} está en el pasillo de bebidas, junto a las demás gaseosas.",
    "Hoy compré {envase} y el precio subió un poco, ahora cuesta {precio}.",
    "{lugar} solo vendían {envase}.",
    "Mañana abren una tienda nueva y van a vender {m} a {precio}.",
    "La {m} {variante} viene en presentación nueva.",
    "Vi un anuncio de {m} {lugar}.",
    "Se agotó {m} {lugar} {momento}.",
    "Estoy armando la lista para la reunión: {envase} y {comida}.",
    "Mi tienda recibió {m} {variante} esta semana.",
    "Leí que {m} va a lanzar una presentación nueva.",
    "En el domicilio pedí {envase} con {comida}.",
    "La {m} se consigue {lugar} a {precio}.",
    "{persona} me preguntó dónde comprar {m}.",
    "Hoy el domiciliario trajo {envase} junto con {comida}.",
    "Pasé {lugar} y vi la nevera con {m} y otras bebidas.",
    "El vendedor dijo que la {m} {variante} llega la próxima semana.",
    "Me pidieron {envase} para el almuerzo y la conseguí {lugar}.",
    "En el menú del día incluían {envase} con {comida}.",
    # --- extra
    "{clima} vi {envase} {lugar}.",
    "Pagué {precio} por {envase}.",
    "{persona} y yo compramos {envase} {evento}.",
    "La {m} {variante} estaba disponible {lugar}.",
    "Llegué {lugar} y había {envase} en la nevera.",
    "{evento} sirvieron {m}, entre otras bebidas.",
    "Le pregunté a {persona} si había {m} {variante} y dijo que sí.",
    "{m} es una de las gaseosas que más se ven {lugar}.",
    "Pedí {envase} con {comida} {momento}.",
    "Probé {envase} {lugar} para comparar con otra.",
]

# Dos marcas: {m} es la marca elegida y {m2} otra marca distinta del registro.
COMPARATIVOS = [
    "{m} vs {m2}: ¿cuál prefieren?",
    "En la tienda {m} y {m2} cuestan lo mismo, {precio} cada una.",
    "Hoy vi {m} y {m2} en la misma nevera {lugar}.",
    "Pregunta seria: ¿{m} o {m2} para acompañar {comida}?",
    "Encuesta rápida: {m} o {m2}.",
    "{lugar} solo tenían {m} y {m2}.",
    "Compré {envase} y una {m2} para tener de las dos.",
    "{persona} pidió {m} y yo pedí {m2}.",
    "El supermercado tiene {m} y {m2} en oferta esta semana.",
    "Entre {m} y {m2} hay diferencia de {precio} en la tienda.",
    "¿Alguien sabe cuál viene más grande, la {m} o la {m2}?",
    "{evento} sirvieron {m} y {m2}.",
    "Estoy leyendo la tabla nutricional de {m} y de {m2}.",
    "{m} y {m2}, dos de las gaseosas que más se ven en las tiendas colombianas.",
    "{momento} me tomé {m} y {persona} una {m2}.",
    "Debate de hoy en la oficina: {m} contra {m2}.",
    "¿Cuál se vende más {lugar}, {m} o {m2}?",
    "Hoy {m} y {m2} salieron en promoción al mismo tiempo.",
]

REACCIONES = [
    "Eso es todo.", "Lo dejo por aquí.", "Dato para quien le sirva.", "Por si les interesa.",
    "Aviso a los interesados.", "Quedo atento a sus respuestas.", "Cualquier dato me avisan.",
    "Eso fue lo que vi.", "Lo comparto sin más.", "Sigo pendiente.", "A quien le sirva.",
    "Ahí queda el dato.", "Es lo que hay por ahora.", "¿Alguien más lo notó?", "¿Alguien más lo vio?",
    "Pasen la voz.", "Me avisan si saben algo más.", "Esa fue la información.", "Lo anoté para la próxima.",
    "Queda registrado.", "Lo confirmo mañana.", "Les cuento si cambia algo.", "Gracias por leer.",
    "Les aviso cuando sepa más.", "Eso fue lo que pasó.", "Lo vi con mis propios ojos.",
    "Para que lo tengan en cuenta.", "Tomen nota.", "Eso ya lo sabíamos.", "Nada más por ahora.",
]

CIERRES = [
    "", "", "", "", "", " 🥤", " 🛒", " 📍", " 👀", " 🤔", " 🧾", " Saludos.", " Gracias.", " Punto.",
    " #Colombia", " #{m}", " #Gaseosas", " #Tienda", " #Precio", " #Supermercado", " #Gaseosa",
    " Atentos.", " Ojo.", " Nada más.", " Sin más.", " Eso es todo.",
]

CONTEXTOS = [
    "Eso fue {evento}.", "Costaba {precio}.", "Se lo conté a {persona}.", "Eso fue {clima}.",
    "Lo vi {momento}.", "Estaba {lugar}.", "Había también {comida} en la mesa.",
    "La presentación era {variante}.", "Se lo comenté a {persona}.", "Fue {lugar}.",
    "Pasó {momento}.", "Estaba junto a otras gaseosas.",
]

GEN_VERBOS = ["Vi", "Compré", "Encontré", "Busqué", "Pedí", "Pregunté por", "Revisé el precio de",
              "Leí la etiqueta de", "Vi el anuncio de", "Pedí domicilio con", "Conseguí", "Pagué por"]
GEN_COMPL = ["{momento}", "{lugar}", "con {comida}", "{clima}", "{evento}", "a {precio}", "por {precio}",
             "en su versión {variante}", "en presentación {variante}", "para la reunión",
             "para el almuerzo", "para llevar", "con {persona}", "{momento} con {persona}",
             "{lugar} con {comida}", "en la tienda de siempre", "esta semana", "hoy mismo"]

DATA = {
    "aperturas": APERTURAS, "nucleos": NUCLEOS, "comparativos": COMPARATIVOS,
    "reacciones": REACCIONES, "cierres": CIERRES, "contextos": CONTEXTOS,
    "gen_verbos": GEN_VERBOS, "gen_compl": GEN_COMPL,
    "interjecciones": ["Oigan ", "Ojo ", "Mano ", "Parce ", "Hola ", "Buenas "],
    "emojis": ["🤔", "👀", "📍", "🛒", "🥤", "🧾"],
    "ruido_prefijo": "oigan ",
}
