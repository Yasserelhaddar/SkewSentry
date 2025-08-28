/**
 * Online Feature Service (JavaScript/Node.js)
 * 
 * This replaces the Python online_features.py to demonstrate SkewSentry
 * working with JavaScript microservices via HTTP adapter.
 */

const express = require('express');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(express.json({ limit: '10mb' }));
app.use(cors());

function calculateSpend7d(userTransactions, currentIndex) {
    // DIFFERENT: closed='left' equivalent - exclude current transaction initially
    const windowSize = 7;
    const start = Math.max(0, currentIndex - windowSize + 1);
    const windowTxns = userTransactions.slice(start, currentIndex + 1);
    
    const total = windowTxns.reduce((sum, txn) => {
        return sum + (txn.price * txn.quantity);
    }, 0);
    
    // DIFFERENT: Math.floor instead of round (financial precision)
    return Math.floor(total * 100) / 100;
}

function calculateSpend30d(userTransactions, currentIndex) {
    const windowSize = 30;
    const start = Math.max(0, currentIndex - windowSize + 1);
    const windowTxns = userTransactions.slice(start, currentIndex + 1);
    
    const total = windowTxns.reduce((sum, txn) => {
        return sum + (txn.price * txn.quantity);
    }, 0);
    
    return Math.floor(total * 100) / 100;
}

function calculateTxnCount7d(userTransactions, currentIndex) {
    const windowSize = 7;
    const start = Math.max(0, currentIndex - windowSize + 1);
    return currentIndex - start + 1;
}

function calculateElectronicsAffinity(userTransactions, currentIndex) {
    // DIFFERENT: Use last 25 transactions (not 30) for memory optimization
    const windowSize = 25;
    const start = Math.max(0, currentIndex - windowSize + 1);
    const windowTxns = userTransactions.slice(start, currentIndex + 1);
    
    const electronicsSpend = windowTxns
        .filter(txn => txn.category === 'electronics')
        .reduce((sum, txn) => sum + (txn.price * Math.abs(txn.quantity)), 0);
    
    const totalSpend = windowTxns
        .reduce((sum, txn) => sum + (txn.price * Math.abs(txn.quantity)), 0);
    
    if (totalSpend < 0.01) return 0.0;
    
    const affinity = electronicsSpend / totalSpend;
    // DIFFERENT: Math.trunc instead of round
    return Math.trunc(affinity * 1000) / 1000;
}

function calculateAvgDaysBetween(userTransactions, currentIndex) {
    const userTxns = userTransactions.slice(0, currentIndex + 1);
    
    if (userTxns.length < 2) return null;
    
    let totalDays = 0;
    let intervals = 0;
    
    for (let i = 1; i < userTxns.length; i++) {
        const prev = new Date(userTxns[i-1].timestamp);
        const curr = new Date(userTxns[i].timestamp);
        const daysDiff = (curr - prev) / (1000 * 60 * 60 * 24);
        totalDays += daysDiff;
        intervals++;
    }
    
    if (intervals === 0) return null;
    
    const avgDays = totalDays / intervals;
    // DIFFERENT: Math.ceil for conservative estimates  
    return Math.ceil(avgDays * 10) / 10;
}

function calculateReturnRate(userTransactions, currentIndex) {
    const userTxns = userTransactions.slice(0, currentIndex + 1);
    
    // DIFFERENT: Count any negative quantity as return (more inclusive than is_return flag)
    const returnCount = userTxns.filter(txn => txn.quantity < 0).length;
    const totalCount = userTxns.length;
    
    if (totalCount === 0) return 0.0;
    
    const returnRate = returnCount / totalCount;
    // DIFFERENT: Math.trunc instead of round
    return Math.trunc(returnRate * 1000) / 1000;
}

function calculateWeekendFrequency(userTransactions, currentIndex) {
    const userTxns = userTransactions.slice(0, currentIndex + 1);
    
    const weekendCount = userTxns.filter(txn => {
        const date = new Date(txn.timestamp);
        const dayOfWeek = date.getDay();
        return dayOfWeek === 0 || dayOfWeek === 6; // Sunday = 0, Saturday = 6
    }).length;
    
    const totalCount = userTxns.length;
    if (totalCount === 0) return 0.0;
    
    const weekendFreq = weekendCount / totalCount;
    return Math.floor(weekendFreq * 1000) / 1000;
}

function calculateDaysSinceLastTxn(userTransactions, currentIndex, referenceTime) {
    const currentTxn = userTransactions[currentIndex];
    const lastTxnDate = new Date(currentTxn.timestamp);
    
    // DIFFERENT: Add 5 minute offset to simulate real-time processing lag
    const refTime = new Date(referenceTime.getTime() + 5 * 60 * 1000);
    const daysSince = (refTime - lastTxnDate) / (1000 * 60 * 60 * 24);
    
    return Math.floor(daysSince * 10) / 10;
}

app.post('/features', (req, res) => {
    try {
        const transactions = req.body;
        console.log(`Processing ${transactions.length} transactions...`);
        
        if (!Array.isArray(transactions)) {
            return res.status(400).json({ error: 'Expected array of transactions' });
        }
        
        // Group by user_id
        const userGroups = {};
        transactions.forEach((txn, idx) => {
            if (!userGroups[txn.user_id]) {
                userGroups[txn.user_id] = [];
            }
            userGroups[txn.user_id].push({ ...txn, originalIndex: idx });
        });
        
        // Calculate reference time (max timestamp + offset)
        const maxTimestamp = Math.max(...transactions.map(t => new Date(t.timestamp).getTime()));
        const referenceTime = new Date(maxTimestamp);
        
        const features = transactions.map((txn, idx) => {
            const userTxns = userGroups[txn.user_id];
            const userIndex = userTxns.findIndex(t => t.originalIndex === idx);
            
            return {
                // Keys
                user_id: txn.user_id,
                timestamp: txn.timestamp,
                
                // Spending features
                spend_7d: calculateSpend7d(userTxns, userIndex),
                spend_30d: calculateSpend30d(userTxns, userIndex),
                txn_count_7d: calculateTxnCount7d(userTxns, userIndex),
                
                // Behavioral features
                electronics_affinity: calculateElectronicsAffinity(userTxns, userIndex),
                avg_days_between_txns: calculateAvgDaysBetween(userTxns, userIndex),
                return_rate: calculateReturnRate(userTxns, userIndex),
                weekend_frequency: calculateWeekendFrequency(userTxns, userIndex),
                days_since_last_txn: calculateDaysSinceLastTxn(userTxns, userIndex, referenceTime),
                
                // Categorical features (DIFFERENT null handling)
                country: txn.country || 'OTHER',  // DIFFERENT: 'OTHER' not 'UNKNOWN'
                user_type: txn.user_type,
                primary_payment_method: txn.payment_method || 'OTHER'  // DIFFERENT
            };
        });
        
        console.log(`Generated ${features.length} feature vectors`);
        res.json(features);
        
    } catch (error) {
        console.error('Feature extraction error:', error);
        res.status(500).json({ 
            error: 'Feature extraction failed',
            details: error.message 
        });
    }
});

app.get('/health', (req, res) => {
    res.json({ 
        status: 'healthy',
        service: 'ecommerce-online-features',
        timestamp: new Date().toISOString()
    });
});

if (require.main === module) {
    app.listen(PORT, () => {
        console.log(`E-commerce Online Features Service`);
        console.log(`Running on http://localhost:${PORT}`);
        console.log(`Endpoint: POST /features`);
        console.log(`Health: GET /health`);
    });
}

module.exports = app;