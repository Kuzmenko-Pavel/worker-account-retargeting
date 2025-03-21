CREATE TABLE IF NOT EXISTS Offer2Rating
(
id INT8 PRIMARY KEY,
rating DECIMAL(10,4),
FOREIGN KEY(id) REFERENCES Offer(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_Offer2Rating_id_rat ON Offer2Rating (id DESC, rating DESC);
