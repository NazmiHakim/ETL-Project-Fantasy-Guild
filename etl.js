const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const keyMaps = {
    hunter: {},
    quest: {},
    client: {},
    receptionist: {},
    date: {},
};

function query(db, sql, params = []) {
    return new Promise((resolve, reject) => {
        db.all(sql, params, (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
        });
    });
}

function runStmt(stmt, params = []) {
    return new Promise((resolve, reject) => {
        stmt.run(params, function(err) {
            if (err) reject(err);
            else resolve({ lastID: this.lastID });
        });
    });
}

async function clearTable(db, tableName) {
    console.log(`- Clearing table: ${tableName}.`);
    await query(db, `DELETE FROM ${tableName}`);
    await query(db, `DELETE FROM sqlite_sequence WHERE name = '${tableName}'`);
}

async function etlDimHunter(oltpDb, dwhDb) {
    console.log('\nETL for DimHunter');
    const sourceData = await query(oltpDb, `
        SELECT
            h.id AS HunterID, h.first_name, h.last_name, h.alias, h.level, h.status,
            r.race_name AS RaceName, hc.class_name AS ClassName, hr.rank_name AS HunterRankName,
            hr.rank_order AS HunterRankOrder, g.name AS GuildName, g.location AS GuildLocation
        FROM Hunter h
        LEFT JOIN Race r ON h.race_id = r.id
        LEFT JOIN Hunter_Class hc ON h.class_id = hc.id
        LEFT JOIN Hunter_Rank hr ON h.rank_id = hr.id
        LEFT JOIN Guild g ON h.guild_id = g.id;
    `);
    console.log(`- Extracted ${sourceData.length} hunter records.`);

    await clearTable(dwhDb, 'DimHunter');
    const insertSql = `
        INSERT INTO DimHunter (HunterID, FullName, Alias, Level, Status, RaceName, ClassName, HunterRankName, HunterRankOrder, GuildName, GuildLocation)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    `;
    const stmt = dwhDb.prepare(insertSql);
    for (const row of sourceData) {
        const fullName = `${row.first_name || ''} ${row.last_name || ''}`.trim();
        const { lastID } = await runStmt(stmt, [
            row.HunterID, fullName, row.Alias, row.level, row.status, row.RaceName,
            row.ClassName, row.HunterRankName, row.HunterRankOrder, row.GuildName, row.GuildLocation
        ]);
        keyMaps.hunter[row.HunterID] = lastID;
    }
    stmt.finalize();
    console.log(`- Loaded ${sourceData.length} records into DimHunter.`);
}

async function etlDimQuest(oltpDb, dwhDb) {
    console.log('\nETL for DimQuest');
    const sourceData = await query(oltpDb, `
        SELECT
            q.id AS QuestID, q.quest_name AS QuestName,
            qr.rank_name AS QuestRankName, qs.status_name AS QuestStatusName,
            hr.rank_name AS RecommendedHunterRank
        FROM Quest q
        LEFT JOIN Quest_Rank qr ON q.quest_rank_id = qr.id
        LEFT JOIN Quest_Status qs ON q.quest_status_id = qs.id
        LEFT JOIN Hunter_Rank hr ON qr.recommended_hunter_rank_id = hr.id;
    `);
    console.log(`- Extracted ${sourceData.length} quest records.`);

    await clearTable(dwhDb, 'DimQuest');
    const insertSql = `INSERT INTO DimQuest (QuestID, QuestName, QuestRankName, QuestStatusName, RecommendedHunterRank) VALUES (?, ?, ?, ?, ?);`;
    const stmt = dwhDb.prepare(insertSql);
    for (const row of sourceData) {
        const { lastID } = await runStmt(stmt, [row.QuestID, row.QuestName, row.QuestRankName, row.QuestStatusName, row.RecommendedHunterRank]);
        keyMaps.quest[row.QuestID] = lastID;
    }
    stmt.finalize();
    console.log(`- Loaded ${sourceData.length} records into DimQuest.`);
}

async function etlDimClient(oltpDb, dwhDb) {
    console.log('\nETL for DimClient');
    const sourceData = await query(oltpDb, `SELECT id AS ClientID, name AS ClientName FROM Client;`);
    console.log(`- Extracted ${sourceData.length} client records.`);

    await clearTable(dwhDb, 'DimClient');
    const insertSql = `INSERT INTO DimClient (ClientID, ClientName) VALUES (?, ?);`;
    const stmt = dwhDb.prepare(insertSql);
    for (const row of sourceData) {
        const { lastID } = await runStmt(stmt, [row.ClientID, row.ClientName]);
        keyMaps.client[row.ClientID] = lastID;
    }
    stmt.finalize();
    console.log(`- Loaded ${sourceData.length} records into DimClient.`);
}

async function etlDimReceptionist(oltpDb, dwhDb) {
    console.log('\nETL for DimReceptionist');
    const sourceData = await query(oltpDb, `
        SELECT r.id AS ReceptionistID, r.first_name, r.last_name, g.name AS GuildName
        FROM Receptionist r LEFT JOIN Guild g ON r.guild_id = g.id;
    `);
    console.log(`- Extracted ${sourceData.length} receptionist records.`);

    await clearTable(dwhDb, 'DimReceptionist');
    const insertSql = `INSERT INTO DimReceptionist (ReceptionistID, FullName, GuildName) VALUES (?, ?, ?);`;
    const stmt = dwhDb.prepare(insertSql);
    for (const row of sourceData) {
        const fullName = `${row.first_name || ''} ${row.last_name || ''}`.trim();
        const { lastID } = await runStmt(stmt, [row.ReceptionistID, fullName, row.GuildName]);
        keyMaps.receptionist[row.ReceptionistID] = lastID;
    }
    stmt.finalize();
    console.log(`- Loaded ${sourceData.length} records into DimReceptionist.`);
}

async function etlDimDate(oltpDb, dwhDb) {
    console.log('\nStarting ETL for DimDate');
    const dates = await query(oltpDb, `
        SELECT DISTINCT date_posted AS dt FROM Quest WHERE dt IS NOT NULL
        UNION
        SELECT DISTINCT date_completed AS dt FROM Quest WHERE dt IS NOT NULL;
    `);
    console.log(`- Extracted ${dates.length} unique dates.`);

    await clearTable(dwhDb, 'DimDate');
    const insertSql = `INSERT INTO DimDate (DateKey, FullDate, DayOfWeek, DayOfMonth, Month, MonthName, Quarter, Year) VALUES (?, ?, ?, ?, ?, ?, ?, ?);`;
    const stmt = dwhDb.prepare(insertSql);
    const monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
    const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    let loadedCount = 0;

    for (const item of dates) {
        const d = new Date(item.dt);
        if (isNaN(d.getTime())) continue;

        const year = d.getFullYear();
        const month = d.getMonth() + 1;
        const day = d.getDate();
        const dateKey = parseInt(`${year}${String(month).padStart(2, '0')}${String(day).padStart(2, '0')}`);

        if (keyMaps.date[dateKey]) continue;

        await runStmt(stmt, [
            dateKey, `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`,
            dayNames[d.getDay()], day, month, monthNames[d.getMonth()],
            Math.floor((d.getMonth() + 3) / 3), year
        ]);
        keyMaps.date[dateKey] = dateKey;
        loadedCount++;
    }
    stmt.finalize();
    console.log(`- Loaded ${loadedCount} records into DimDate.`);
}

async function etlFactQuestAssignments(oltpDb, dwhDb) {
    console.log('\nETL for FactQuestAssignments');
    const sourceData = await query(oltpDb, `
        SELECT
            hq.quest_id, hq.hunter_id, hq.is_party_leader,
            q.client_id, q.posted_by_receptionist_id, q.date_posted, q.date_completed, q.reward_gold
        FROM Hunter_Quest hq
        JOIN Quest q ON hq.quest_id = q.id
        WHERE q.date_completed IS NOT NULL;
    `);
    console.log(`- Extracted ${sourceData.length} quest assignment records.`);

    await clearTable(dwhDb, 'FactQuestAssignments');
    const insertSql = `
        INSERT INTO FactQuestAssignments (QuestKey, HunterKey, ClientKey, ReceptionistKey, DatePostedKey, DateCompletedKey, RewardGold, QuestDurationDays, IsPartyLeader, AssignmentCount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    `;
    const stmt = dwhDb.prepare(insertSql);
    for (const row of sourceData) {
        const datePosted = new Date(row.date_posted);
        const dateCompleted = new Date(row.date_completed);

        if (isNaN(datePosted.getTime()) || isNaN(dateCompleted.getTime())) continue;
        
        const questKey = keyMaps.quest[row.quest_id];
        const hunterKey = keyMaps.hunter[row.hunter_id];
        if (!questKey || !hunterKey) continue;
        
        const clientKey = keyMaps.client[row.client_id];
        const receptionistKey = keyMaps.receptionist[row.posted_by_receptionist_id];
        const datePostedKey = parseInt(`${datePosted.getFullYear()}${String(datePosted.getMonth() + 1).padStart(2, '0')}${String(datePosted.getDate()).padStart(2, '0')}`);
        const dateCompletedKey = parseInt(`${dateCompleted.getFullYear()}${String(dateCompleted.getMonth() + 1).padStart(2, '0')}${String(dateCompleted.getDate()).padStart(2, '0')}`);
        const duration = Math.ceil((dateCompleted - datePosted) / (1000 * 60 * 60 * 24));

        await runStmt(stmt, [
            questKey, hunterKey, clientKey, receptionistKey, datePostedKey, dateCompletedKey,
            row.reward_gold, duration, row.is_party_leader ? 1 : 0, 1
        ]);
    }
    stmt.finalize();
    console.log(`- Loaded records into FactQuestAssignments.`);
}

async function runETL() {
    console.log('ETL PROCESS STARTED');
    console.time('Total ETL time');

    const dbPath = __dirname;
    const oltpDb = new sqlite3.Database(path.join(dbPath, 'guild_oltp.db'), sqlite3.OPEN_READONLY);
    const dwhDb = new sqlite3.Database(path.join(dbPath, 'guild_dwh.db'));

    try {
        await etlDimHunter(oltpDb, dwhDb);
        await etlDimQuest(oltpDb, dwhDb);
        await etlDimClient(oltpDb, dwhDb);
        await etlDimReceptionist(oltpDb, dwhDb);
        await etlDimDate(oltpDb, dwhDb);
        await etlFactQuestAssignments(oltpDb, dwhDb);

        console.log('\nETL PROCESS COMPLETED');
    } catch (err) {
        console.error('\nETL process failed:', err.message);
    } finally {
        oltpDb.close();
        dwhDb.close();
        console.log('Database connections closed.');
        console.timeEnd('Total ETL time');
    }
}

runETL();