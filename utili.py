#esempi DAO con query annesse
@staticmethod
def getYears():
    cnx = DBConnect.get_connection()
    res = []
    if cnx is None:
        print("Connessione fallita")
    else:
        cursor = cnx.cursor(dictionary=True)
        query = """select distinct s.`year` as anno
                from seasons s 
                order by s.`year`"""
        cursor.execute(query)
        for row in cursor:
            res.append(row["anno"])

        cursor.close()
        cnx.close()
    return res

@staticmethod
def getNodes(anno):
    cnx = DBConnect.get_connection()
    res = []
    if cnx is None:
        print("Connessione fallita")
    else:
        cursor = cnx.cursor(dictionary=True)
        query = """select distinct d.*
                from drivers d,races r,results r2 
                where d.driverId =r2.driverId 
                and r.raceId =r2.raceId 
                and year (r.`date`)=%s
                and r2.`position` is not null"""
        cursor.execute(query,(anno,))
        for row in cursor:
            res.append((Driver(**row)))

        cursor.close()
        cnx.close()
    return res


@staticmethod
def getEdges(anno,idMap):
    cnx = DBConnect.get_connection()
    res = []
    if cnx is None:
        print("Connessione fallita")
    else:
        cursor = cnx.cursor(dictionary=True)
        query = """select r1.driverId as id1, r2.driverId as id2, count(*) as peso
            from results as r1, results as r2, races
            where r1.raceId = r2.raceId
            and races.raceId = r1.raceId
            and races.year = %s
            and r1.position is not null
            and r2.position is not null 
            and r1.position < r2.position 
            group by id1, id2"""
        cursor.execute(query,(anno,))
        for row in cursor:
            res.append((idMap[row["id1"]],idMap[row["id2"]],row["peso"]))

        cursor.close()
        cnx.close()
    return res


#model(grafo,getNN, getNE ecc)
def buildGraph(self, anno):
    self._grafo.clear()
    nodi = DAO.getNodes(anno)
    self._grafo.add_nodes_from(nodi)
    for n in self._grafo.nodes:
        self._idMap[n.driverId] = n
    edges = DAO.getEdges(anno, self._idMap)
    for e in edges:
        self._grafo.add_edge(e[0], e[1], weight=e[2])


def getNumNodes(self):
    return len(list(self._grafo.nodes))


def getNumEdges(self):
    return len(list(self._grafo.edges))

#prendo gli archi con peso maggiore fino a un certo numero (in questo caso i tre più pesanti)
def ArchiPesoMaggiore(self):
    archi=self.grafo.edges(data=True)
    archiOrd=sorted(archi, key=lambda x: x[2]['weight'], reverse=True)
    return archiOrd[:3]

#trovo il max percorso partendo da un nodo
def getMaxPercorso(self,nodo):
    newNodo=self._idMap[nodo]
    return dfs_tree(self._grafo,newNodo)


#controller: fillDD, creaGrafo: PULISCI SEMPRE E AGGIORNA!!!
def fillDDYear(self):
    self._view._ddAnno.options.clear()
    anni=self._model.getYears()
    for a in anni:
        self._view._ddAnno.options.append(ft.dropdown.Option(a))
    self._view.update_page()

def handleCreaGrafo(self,e):
    anno=self._view._ddAnno.value
    self._view.txt_result.controls.clear()
    self._model.buildGraph(anno)
    self._view.txt_result.controls.append(ft.Text(f"Grafo correttamente creato:"))
    self._view.txt_result.controls.append(ft.Text(f"Numero di nodi: {self._model.getNumNodes()}"))
    self._view.txt_result.controls.append(ft.Text(f"Numero di archi: {self._model.getNumEdges()}"))

    pilota,score=self._model.getBestDriver()
    self._view.txt_result.controls.append(ft.Text(f"Best driver: {pilota}, with score {score}"))

    self._view.update_page()