#ricorsione percorso di peso massimo
def getBestPath(self, startStr):
    self._bestPath = []
    self._bestScore = 0

    start = self._idMap[int(startStr)]

    parziale = [start]

    vicini = self._graph.neighbors(start)
    for v in vicini:
        parziale.append(v)
        self._ricorsione(parziale)
        parziale.pop()

    return self._bestPath, self._bestScore


def _ricorsione(self, parziale):
    if self.getScore(parziale) > self._bestScore:
        self._bestScore = self.getScore(parziale)
        self._bestPath = copy.deepcopy(parziale)

    for v in self._graph.neighbors(parziale[-1]):
        if (v not in parziale and  # check if not in parziale
                self._graph[parziale[-2]][parziale[-1]]["weight"] >
                self._graph[parziale[-1]][v]["weight"]):  # check if peso nuovo arco è minore del precedente
            parziale.append(v)
            self._ricorsione(parziale)
            parziale.pop()


def getScore(self, listOfNodes):
    tot = 0
    for i in range(len(listOfNodes) - 1):
        tot += self._graph[listOfNodes[i]][listOfNodes[i + 1]]["weight"]

    return tot


#ricorsione dream team: team di k piloti con minimo tasso di sconfitta
def getDreamTeam(self, k):
    self._bestPath = []
    self._bestScore = 1000

    parziale = []
    self._ricorsione(parziale, k)
    return self._bestPath, self._bestScore

def _ricorsione(self, parziale, k):
    if len(parziale) == k:
        if self.getScore(parziale) < self._bestScore:
            self._bestScore = self.getScore(parziale)
            self._bestPath = copy.deepcopy(parziale)
        return

    for n in self._graph.nodes():
        if n not in parziale:
            parziale.append(n)
            self._ricorsione(parziale, k)
            parziale.pop()

def getScore(self, team):
    score = 0
    for e in self._graph.edges(data=True):
        if e[0] not in team and e[1] in team:
            score += e[2]["weight"]
    return score


#ricorsione per  creare un set con maggior numero possibile di album (nodi) con durata complessiva dtot
def getSetOfNodes(self, a1, soglia):
    self._bestSet = {}
    self._maxLen = 0

    parziale = {a1}
    cc = nx.node_connected_component(self._grafo, a1)

    cc.remove(a1)

    for n in cc:
        # richiamo la mia ricorsione
        parziale.add(n)
        cc.remove(n)
        self._ricorsione(parziale, cc, soglia)
        cc.add(n)
        parziale.remove(n)

    return self._bestSet, self._getDurataTot(self._bestSet)

def _ricorsione(self, parziale, rimanenti, soglia):

    # 1) verifico che parziale sia una soluzione ammissibile, ovvero se viola i vincoli.
    if self._getDurataTot(parziale) > soglia:
        return

    # 2) se parziale soddisfa i criteri, allora verifico se è migliore di bestSet
    if len(parziale) > self._maxLen:
        self._maxLen = len(parziale)
        self._bestSet = copy.deepcopy(parziale)

    # 3) aggiungo e faccio ricorsione
    for r in rimanenti:
        parziale.add(r)
        rimanenti.remove(r)
        self._ricorsione(parziale, rimanenti, soglia)
        parziale.remove(r)
        rimanenti.add(r)


#ricorsione per percorso di peso massimo(vertici una volta, peso archi strett decr)
def getBestPath(self, start):
    self._bestPath = []
    self._bestScore = 0

    parziale = [start]

    vicini = self._grafo.neighbors(start)
    for v in vicini:
        parziale.append(v)
        self._ricorsione(parziale)
        parziale.pop()

    return self._bestPath, self._bestScore

def _ricorsione(self, parziale):
    print(len(parziale))
    # 1) verifico che parziale sia una soluzione, e verifico se migliore della best
    if self.score(parziale) > self._bestScore:
        self._bestScore = self.score(parziale)
        self._bestPath = copy.deepcopy(parziale)

    # 2) verifico se posso aggiungere un nuovo nodo
    # 3) aggiungo nodo e faccio ricorsione

    for v in self._grafo.neighbors(parziale[-1]):
        if (v not in parziale and
                self._grafo[parziale[-2]][parziale[-1]]["weight"] >
                self._grafo[parziale[-1]][v]["weight"]):
            parziale.append(v)
            self._ricorsione(parziale)
            parziale.pop()

def score(self, listOfNodes):
    if len(listOfNodes) < 2:
        warnings.warn("Errore in score, attesa lista lunga almeno 2.")

    totPeso = 0
    for i in range(len(listOfNodes) - 1):
        totPeso += self._grafo[listOfNodes[i]][listOfNodes[i + 1]]["weight"]

    return totPeso


#ricorsione per percorso più lungo in termini di num archi(no peso, peso maggiore di tutti gli altri aggiunti al percorso)
def searchPath(self, product_number):
    nodoSource = self.idMap[product_number]
    parziale = []
    self.ricorsione(parziale, nodoSource, 0)
    print("final", len(self._solBest), [i[2]["weight"] for i in self._solBest])

def ricorsione(self, parziale, nodoLast, livello):
    archiViciniAmmissibili = self.getArchiViciniAmm(nodoLast, parziale)

    if len(archiViciniAmmissibili) == 0:
        if len(parziale) > len(self._solBest):
            self._solBest = list(parziale)
            print(len(self._solBest), [ii[2]["weight"] for ii in self._solBest])

    for a in archiViciniAmmissibili:
        parziale.append(a)
        self.ricorsione(parziale, a[1], livello + 1)
        parziale.pop()

def getArchiViciniAmm(self, nodoLast, parziale):

    archiVicini = self._grafo.edges(nodoLast, data=True)
    result = []
    for a1 in archiVicini:
        if self.isAscendent(a1, parziale) and self.isNovel(a1, parziale):
            result.append(a1)
    return result

def isAscendent(self, e, parziale):
    if len(parziale) == 0:
        print("parziale is empty in isAscendent")
        return True
    return e[2]["weight"] >= parziale[-1][2]["weight"]

def isNovel(self, e, parziale):
    if len(parziale) == 0:
        print("parziale is empty in isnovel")
        return True
    e_inv = (e[1], e[0], e[2])
    return (e_inv not in parziale) and (e not in parziale)


#ricorsione che massimizza la somma dei pesi degli archi attraversati
def getCamminoOttimo(self, v0, v1, t):
    self._bestPath = []
    self._bestObjFun = 0

    parziale = [v0]

    self._ricorsione(parziale, v1, t)

    return self._bestPath, self._bestObjFun

def _ricorsione(self, parziale, v1, t):
    # Verificare se parziale è una possibile soluzione
    # verificare se parziale è meglio del best
    # esco
    if parziale[-1] == v1:
        if self.getObjFun(parziale) > self._bestObjFun:
            self._bestObjFun = self.getObjFun(parziale)
            self._bestPath = copy.deepcopy(parziale)

    if len(parziale) == t + 1:
        return

    # Posso ancora aggiungere nodi
    # prendo i vicini e aggiungo un nodo alla volta
    # ricorsione
    for n in self._graph.neighbors(parziale[-1]):
        if n not in parziale:
            parziale.append(n)
            self._ricorsione(parziale, v1, t)
            parziale.pop()

def getObjFun(self, listOfNodes):
    objval = 0
    for i in range(0, len(listOfNodes) - 1):
        objval += self._graph[listOfNodes[i]][listOfNodes[i + 1]]["weight"]
    return objval


#ricorsione che massimizza il punteggio
def cammino_ottimo(self):
    self._cammino_ottimo = []
    self._score_ottimo = 0
    self._occorrenze_mese = dict.fromkeys(range(1, 13), 0)

    for nodo in self._nodes:
        self._occorrenze_mese[nodo.datetime.month] += 1
        successivi_durata_crescente = self._calcola_successivi(nodo)
        self._calcola_cammino_ricorsivo([nodo], successivi_durata_crescente)
        self._occorrenze_mese[nodo.datetime.month] -= 1
    return self._cammino_ottimo, self._score_ottimo

def _calcola_cammino_ricorsivo(self, parziale: list[Sighting], successivi: list[Sighting]):
    if len(successivi) == 0:
        score = Model._calcola_score(parziale)
        if score > self._score_ottimo:
            self._score_ottimo = score
            self._cammino_ottimo = copy.deepcopy(parziale)
    else:
        for nodo in successivi:
            # aggiungo il nodo in parziale ed aggiorno le occorrenze del mese corrispondente
            parziale.append(nodo)
            self._occorrenze_mese[nodo.datetime.month] += 1
            # nuovi successivi
            nuovi_successivi = self._calcola_successivi(nodo)
            # ricorsione
            self._calcola_cammino_ricorsivo(parziale, nuovi_successivi)
            # backtracking: visto che sto usando un dizionario nella classe per le occorrenze, quando faccio il
            # backtracking vado anche a togliere una visita dalle occorrenze del mese corrispondente al nodo che
            # vado a sottrarre
            self._occorrenze_mese[parziale[-1].datetime.month] -= 1
            parziale.pop()

def _calcola_successivi(self, nodo: Sighting) -> list[Sighting]:
    """
    Calcola il sottoinsieme dei successivi ad un nodo che hanno durata superiore a quella del nodo, senza eccedere
    il numero ammissibile di occorrenze per un dato mese
    """
    successivi = self._grafo.successors(nodo)
    successivi_ammissibili = []
    for s in successivi:
        if s.duration > nodo.duration and self._occorrenze_mese[s.datetime.month] < 3:
            successivi_ammissibili.append(s)
    return successivi_ammissibili

@staticmethod
def _calcola_score(cammino: list[Sighting]) -> int:
    """
    Funzione che calcola il punteggio di un cammino.
    :param cammino: il cammino che si vuole valutare.
    :return: il punteggio
    """
    # parte del punteggio legata al numero di tappe
    score = 100 * len(cammino)
    # parte del punteggio legata al mese
    for i in range(1, len(cammino)):
        if cammino[i].datetime.month == cammino[i - 1].datetime.month:
            score += 200
    return score