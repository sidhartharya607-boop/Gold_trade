// Frontend Script for Spread Arbitrage Quant Workstation

const AUTH_TOKEN = "secret_arbitrage_token_2026";
const host = window.location.host || "localhost:7890";
const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
const WS_URL = `${protocol}//${host}/ws/live-data?token=${AUTH_TOKEN}`;

let socket = null;
let reconnectTimeout = null;
let reconnectDelay = 1000;
const MAX_RECONNECT_DELAY = 15000;

// Price tracking variables
let previousPetalLtp = 0.0;
let previousMiniLtp = 0.0;

// DOM Elements - Widgets
const petalLtpEl = document.getElementById("petal-ltp");
const miniLtpEl = document.getElementById("mini-ltp");
const currentSpreadEl = document.getElementById("current-spread");
const virtualPnlEl = document.getElementById("virtual-pnl");
const statusTextEl = document.getElementById("status-text");
const statusBadgeEl = document.getElementById("status-badge");
const winRatioBadge = document.getElementById("win-ratio-badge");
const realizedPnlBadge = document.getElementById("realized-pnl-badge");

// DOM Elements - Position details
const petalEntryInfo = document.getElementById("petal-entry-info");
const miniEntryInfo = document.getElementById("mini-entry-info");
const spreadEntryInfo = document.getElementById("spread-entry-info");
const positionDirBadge = document.getElementById("position-dir-badge");
const petalLegPnl = document.getElementById("petal-leg-pnl");
const miniLegPnl = document.getElementById("mini-leg-pnl");
const petalVolumeEl = document.getElementById("petal-volume");
const petalBuyQtyEl = document.getElementById("petal-buy-qty");
const petalSellQtyEl = document.getElementById("petal-sell-qty");
const miniVolumeEl = document.getElementById("mini-volume");
const miniBuyQtyEl = document.getElementById("mini-buy-qty");
const miniSellQtyEl = document.getElementById("mini-sell-qty");


// DOM Elements - Action Controls
const entryBtn = document.getElementById("entry-btn");
const exitBtn = document.getElementById("exit-btn");
const killSwitchBtn = document.getElementById("kill-switch-btn");
const exportCsvBtn = document.getElementById("export-csv-btn");
const exportManualCsvBtn = document.getElementById("export-manual-csv-btn");
const downloadBtn = document.getElementById("download-btn");
const updateParamsBtn = document.getElementById("update-params-btn");

// DOM Elements - Form Fields
const entryInput = document.getElementById("input-entry");
const targetInput = document.getElementById("input-target");
const slInput = document.getElementById("input-sl");

const checkboxTarget = document.getElementById("checkbox-target");
const inputAutoTargetVal = document.getElementById("input-auto-target-val");

const checkboxSl = document.getElementById("checkbox-sl");
const inputAutoSlVal = document.getElementById("input-auto-sl-val");

const checkboxSquareoff = document.getElementById("checkbox-squareoff");
const inputAutoSquareoffTime = document.getElementById("input-auto-squareoff-time");

const paperModeInput = document.getElementById("input-paper-mode");
const autoTradingInput = document.getElementById("input-auto-trading");

const inputSpreadBuffer = document.getElementById("input-spread-buffer");
const checkboxContractionEntry = document.getElementById("checkbox-contraction-entry");
const checkboxSpreadExit = document.getElementById("checkbox-spread-exit");
const logsContainer = document.getElementById("logs-container");
const historyBody = document.getElementById("history-body");
const manualTradesBody = document.getElementById("manual-trades-body");
const manualTriggerDiff = document.getElementById("manual-trigger-diff");
const capitalInput = document.getElementById("input-capital");
const quantityInput = document.getElementById("input-quantity");

const selectBroker = document.getElementById("select-broker");
const angeloneFields = document.getElementById("angelone-fields");
const angeloneClientId = document.getElementById("input-angelone-client-id");
const angelonePassword = document.getElementById("input-angelone-password");
const angeloneTotp = document.getElementById("input-angelone-totp");
const angeloneApiKey = document.getElementById("input-angelone-api-key");
const angelonePetalSymbol = document.getElementById("input-angelone-petal-symbol");
const angelonePetalToken = document.getElementById("input-angelone-petal-token");
const angeloneMiniSymbol = document.getElementById("input-angelone-mini-symbol");
const angeloneMiniToken = document.getElementById("input-angelone-mini-token");

const growwFields = document.getElementById("groww-fields");
const growwClientId = document.getElementById("input-groww-client-id");
const growwApiKey = document.getElementById("input-groww-api-key");
const growwSecret = document.getElementById("input-groww-secret");
const growwPetalSymbol = document.getElementById("input-groww-petal-symbol");
const growwMiniSymbol = document.getElementById("input-groww-mini-symbol");

// Helper to log console logs locally before connection is established
function logLocalMessage(message) {
    const timestamp = new Date().toLocaleTimeString();
    const div = document.createElement("div");
    div.className = "log-line system-msg";
    div.innerText = `[${timestamp}] ${message}`;
    logsContainer.appendChild(div);
    logsContainer.scrollTop = logsContainer.scrollHeight;
}

// Connect to Backend WebSocket
function connect() {
    if (socket) {
        socket.close();
    }

    logLocalMessage("[SYSTEM] Establishing socket stream...");
    socket = new WebSocket(WS_URL);

    socket.onopen = () => {
        logLocalMessage("[SYSTEM] Market streams connected successfully.");
        reconnectDelay = 1000;
        if (reconnectTimeout) {
            clearTimeout(reconnectTimeout);
            reconnectTimeout = null;
        }
    };

    socket.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            updateDashboard(data);
        } catch (error) {
            console.error("Error parsing WebSocket JSON payload:", error);
        }
    };

    socket.onclose = (event) => {
        let closeReason = "Connection disconnected.";
        if (event.code === 3000) {
            closeReason = "Unauthorized token key verification failed.";
        }
        logLocalMessage(`[SYSTEM] Stream offline. ${closeReason}`);
        
        if (event.code !== 3000) {
            logLocalMessage(`[SYSTEM] Reconnecting in ${(reconnectDelay / 1000).toFixed(1)}s...`);
            reconnectTimeout = setTimeout(() => {
                reconnectDelay = Math.min(reconnectDelay * 1.5, MAX_RECONNECT_DELAY);
                connect();
            }, reconnectDelay);
        }
    };

    socket.onerror = (error) => {
        console.error("WebSocket Connection Error:", error);
    };
}

// Update dashboard elements
function updateDashboard(data) {
    const errorBanner = document.getElementById("connection-error-banner");
    const isApiConnected = data.api_connected;
    const isOffline = !isApiConnected && data.broker === "AngelOne" && !data.paper_trading_mode;
    
    if (errorBanner) {
        if (isOffline) {
            errorBanner.style.display = "flex";
        } else {
            errorBanner.style.display = "none";
        }
    }

    // Update Dynamic Symbol Names
    const petalSymbolNameEl = document.getElementById("petal-symbol-name");
    const miniSymbolNameEl = document.getElementById("mini-symbol-name");
    if (petalSymbolNameEl && data.petal_symbol) {
        petalSymbolNameEl.innerText = data.petal_symbol;
    }
    if (miniSymbolNameEl && data.mini_symbol) {
        miniSymbolNameEl.innerText = data.mini_symbol;
    }

    // 1. Live LTP Price & Tick Flashing
    const currentPetal = data.gold_petal_ltp;
    const currentMini = data.gold_mini_ltp;

    if (isOffline) {
        petalLtpEl.innerText = "Offline";
        miniLtpEl.innerText = "Offline";
    } else {
        petalLtpEl.innerText = currentPetal.toLocaleString('en-IN', { minimumFractionDigits: 2 });
        miniLtpEl.innerText = currentMini.toLocaleString('en-IN', { minimumFractionDigits: 2 });

        if (previousPetalLtp > 0) {
            if (currentPetal > previousPetalLtp) flashElement(petalLtpEl, "tick-up");
            else if (currentPetal < previousPetalLtp) flashElement(petalLtpEl, "tick-down");
        }

        if (previousMiniLtp > 0) {
            if (currentMini > previousMiniLtp) flashElement(miniLtpEl, "tick-up");
            else if (currentMini < previousMiniLtp) flashElement(miniLtpEl, "tick-down");
        }

        previousPetalLtp = currentPetal;
        previousMiniLtp = currentMini;
    }

    // Update Volume & Order book depth split bar gauges
    function updateDepthGauge(prefix, buyQty, sellQty, totalVol) {
        const volumeEl = document.getElementById(`${prefix}-volume`);
        const buyEl = document.getElementById(`${prefix}-buy-qty`);
        const sellEl = document.getElementById(`${prefix}-sell-qty`);
        const ratioEl = document.getElementById(`${prefix}-depth-ratio`);
        const bidBar = document.getElementById(`${prefix}-depth-bid`);
        const askBar = document.getElementById(`${prefix}-depth-ask`);

        if (volumeEl) volumeEl.innerText = (totalVol || 0).toLocaleString();
        if (buyEl) buyEl.innerText = (buyQty || 0).toLocaleString();
        if (sellEl) sellEl.innerText = (sellQty || 0).toLocaleString();

        const totalDepth = (buyQty || 0) + (sellQty || 0);
        let buyPct = 50;
        let sellPct = 50;

        if (totalDepth > 0) {
            buyPct = Math.round((buyQty / totalDepth) * 100);
            sellPct = 100 - buyPct;
        }

        if (ratioEl) ratioEl.innerText = `${buyPct}% / ${sellPct}%`;
        if (bidBar && askBar) {
            bidBar.style.width = `${buyPct}%`;
            askBar.style.width = `${sellPct}%`;
        }
    }

    if (isOffline) {
        const resetGauges = (prefix) => {
            const ratioEl = document.getElementById(`${prefix}-depth-ratio`);
            const bidBar = document.getElementById(`${prefix}-depth-bid`);
            const askBar = document.getElementById(`${prefix}-depth-ask`);
            const volEl = document.getElementById(`${prefix}-volume`);
            const buyEl = document.getElementById(`${prefix}-buy-qty`);
            const sellEl = document.getElementById(`${prefix}-sell-qty`);
            if (ratioEl) ratioEl.innerText = "--% / --%";
            if (bidBar && askBar) {
                bidBar.style.width = "50%";
                askBar.style.width = "50%";
            }
            if (volEl) volEl.innerText = "--";
            if (buyEl) buyEl.innerText = "--";
            if (sellEl) sellEl.innerText = "--";
        };
        resetGauges("petal");
        resetGauges("mini");

        renderOrderBook("petal", null);
        renderOrderBook("mini", null);
        updateTopWorkflowBanner(data);

        currentSpreadEl.innerText = "Disconnected";
        currentSpreadEl.className = "price-value";
    } else {
        updateDepthGauge("petal", data.gold_petal_buy_qty, data.gold_petal_sell_qty, data.gold_petal_volume);
        updateDepthGauge("mini", data.gold_mini_buy_qty, data.gold_mini_sell_qty, data.gold_mini_volume);
        
        renderOrderBook("petal", data.petal_depth);
        renderOrderBook("mini", data.mini_depth);
        updateTopWorkflowBanner(data);

        // 2. Spread Value, Proximity Glow & Visual Progress Gauge
        const spread = data.spread;
        currentSpreadEl.innerText = spread.toLocaleString('en-IN', { minimumFractionDigits: 2 });

        if (spread >= 985) {
            currentSpreadEl.className = "price-value spread-near-target";
        } else if (spread <= 700) {
            currentSpreadEl.className = "price-value spread-near-sl";
        } else {
            currentSpreadEl.className = "price-value";
        }
    }

    // Dynamic Visual Gauge calculation
    const minVal = data.entry_threshold || 850;
    const maxVal = data.target_threshold || 1050;
    const spread = data.spread;
    let percentage = 0;
    if (maxVal > minVal && !isOffline) {
        percentage = ((spread - minVal) / (maxVal - minVal)) * 100;
    }
    percentage = Math.max(0, Math.min(100, percentage));

    const gaugeBar = document.getElementById("spread-gauge-bar");
    const gaugePercent = document.getElementById("gauge-percent");
    const gaugeEntryLabel = document.getElementById("gauge-entry-label");
    const gaugeTargetLabel = document.getElementById("gauge-target-label");

    if (gaugeEntryLabel) gaugeEntryLabel.innerText = Math.round(minVal);
    if (gaugeTargetLabel) gaugeTargetLabel.innerText = Math.round(maxVal);

    if (gaugeBar && gaugePercent) {
        gaugeBar.style.width = `${percentage}%`;
        gaugePercent.innerText = `${Math.round(percentage)}%`;

        if (percentage >= 90) {
            gaugeBar.style.backgroundColor = "var(--accent-green)";
        } else if (spread <= minVal) {
            gaugeBar.style.backgroundColor = "var(--accent-red)";
        } else {
            gaugeBar.style.backgroundColor = "var(--accent-blue)";
        }
    }

    // 3. Header Statistics Badges
    winRatioBadge.innerText = `${data.win_ratio.toFixed(2)}%`;
    
    const realizedPnl = data.realized_pnl;
    realizedPnlBadge.innerText = (realizedPnl >= 0 ? "+" : "") + realizedPnl.toLocaleString('en-IN', { minimumFractionDigits: 2 });
    realizedPnlBadge.className = "badge-value " + (realizedPnl > 0 ? "pnl-profit" : realizedPnl < 0 ? "pnl-loss" : "");

    // 4. Groww Holdings Wealth Summary Update
    const currentValEl = document.getElementById("groww-current-value");
    const totalReturnsEl = document.getElementById("groww-total-returns");
    const growwBalanceEl = document.getElementById("groww-balance");
    const investedCapitalLabel = document.getElementById("invested-capital-label");
    const usedMarginLabel = document.getElementById("used-margin-label");
    
    if (currentValEl && totalReturnsEl && growwBalanceEl && investedCapitalLabel && usedMarginLabel) {
        const totalPnl = data.total_pnl;
        const realizedPnl = data.realized_pnl;
        const totalUnrealizedPnl = totalPnl - realizedPnl;
        
        const investedCapital = data.used_margin;
        const currentValue = investedCapital + totalUnrealizedPnl;
        
        currentValEl.innerText = currentValue.toLocaleString('en-IN', { minimumFractionDigits: 2 });
        
        const returnsPrefix = totalUnrealizedPnl >= 0 ? "+" : "";
        const arrow = totalUnrealizedPnl >= 0 ? "▲" : "▼";
        const returnsPercent = investedCapital > 0 ? (totalUnrealizedPnl / investedCapital) * 100.0 : 0.0;
        const returnsPercentPrefix = returnsPercent >= 0 ? "+" : "";
        
        totalReturnsEl.innerText = `${arrow} ${returnsPrefix}${totalUnrealizedPnl.toLocaleString('en-IN', { minimumFractionDigits: 2 })} (${returnsPercentPrefix}${returnsPercent.toFixed(2)}%)`;
        
        if (totalUnrealizedPnl > 0) {
            totalReturnsEl.className = "groww-returns-value font-mono pnl-profit";
        } else if (totalUnrealizedPnl < 0) {
            totalReturnsEl.className = "groww-returns-value font-mono pnl-loss";
        } else {
            totalReturnsEl.className = "groww-returns-value font-mono";
        }

        growwBalanceEl.innerText = data.available_balance.toLocaleString('en-IN', { minimumFractionDigits: 2 });
        investedCapitalLabel.innerText = investedCapital.toLocaleString('en-IN', { minimumFractionDigits: 2 });
        usedMarginLabel.innerText = data.used_margin.toLocaleString('en-IN', { minimumFractionDigits: 2 });
    }

    // 5. Position Details & Button states
    const status = data.system_status; // "Active", "In-Position", "Halted"
    statusTextEl.innerText = status;
    statusBadgeEl.className = "status-badge";

    if (status === "Active") {
        statusBadgeEl.classList.add("status-active");
        entryBtn.disabled = data.auto_trading_enabled;
        exitBtn.disabled = true;
        if (data.auto_trading_enabled) {
            entryBtn.innerHTML = `Auto Active`;
        } else {
            entryBtn.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" class="btn-icon"><path d="M12 5v14M5 12h14"/></svg> Manual Entry`;
        }
    } else if (status === "In-Position") {
        statusBadgeEl.classList.add("status-in-position");
        entryBtn.disabled = true;
        exitBtn.disabled = false;
        entryBtn.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" class="btn-icon"><path d="M12 5v14M5 12h14"/></svg> Manual Entry`;
    } else if (status === "Halted") {
        statusBadgeEl.classList.add("status-halted");
        entryBtn.disabled = true;
        exitBtn.disabled = true;
        entryBtn.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" class="btn-icon"><path d="M12 5v14M5 12h14"/></svg> Manual Entry`;
    }

    if (positionDirBadge) {
        if (data.is_in_position) {
            positionDirBadge.innerText = `${data.position_direction} (ON)`;
            positionDirBadge.className = "asset-type pnl-profit";
        } else {
            positionDirBadge.innerText = "No Position";
            positionDirBadge.className = "asset-type";
        }
    }
    
    petalEntryInfo.innerText = data.is_in_position ? `Entry: ${data.petal_entry_price.toLocaleString('en-IN', { minimumFractionDigits: 2 })}` : "Entry: --";
    miniEntryInfo.innerText = data.is_in_position ? `Entry: ${data.mini_entry_price.toLocaleString('en-IN', { minimumFractionDigits: 2 })}` : "Entry: --";
    spreadEntryInfo.innerText = data.is_in_position ? `Entry Spread: ${data.entry_spread.toLocaleString('en-IN', { minimumFractionDigits: 2 })}` : "Entry Spread: --";
    
    if (petalLegPnl) {
        if (data.is_in_position) {
            petalLegPnl.innerText = `Petal Leg: ${(data.petal_pnl >= 0 ? "+" : "")}${data.petal_pnl.toFixed(2)}`;
            petalLegPnl.className = data.petal_pnl >= 0 ? "pnl-profit" : "pnl-loss";
        } else {
            petalLegPnl.innerText = "Petal Leg: 0.00";
            petalLegPnl.className = "";
        }
    }
    
    if (miniLegPnl) {
        if (data.is_in_position) {
            miniLegPnl.innerText = `Mini Leg: ${(data.mini_pnl >= 0 ? "+" : "")}${data.mini_pnl.toFixed(2)}`;
            miniLegPnl.className = data.mini_pnl >= 0 ? "pnl-profit" : "pnl-loss";
        } else {
            miniLegPnl.innerText = "Mini Leg: 0.00";
            miniLegPnl.className = "";
        }
    }

    // 6. Form Fields Sync (only updates if user is not currently focusing on the field)
    syncInputField(entryInput, data.entry_threshold);
    syncInputField(targetInput, data.target_threshold);
    syncInputField(slInput, data.sl_threshold);
    syncInputField(capitalInput, data.total_capital);
    syncInputField(quantityInput, data.trade_quantity);
    
    syncCheckboxField(checkboxTarget, data.auto_target_enabled);
    syncInputField(inputAutoTargetVal, data.auto_target_val);
    
    syncCheckboxField(checkboxSl, data.auto_sl_enabled);
    syncInputField(inputAutoSlVal, data.auto_sl_val);
    
    syncCheckboxField(checkboxSquareoff, data.auto_square_off_enabled);
    syncInputField(inputAutoSquareoffTime, data.auto_square_off_time);
    
    syncInputField(inputSpreadBuffer, data.spread_buffer);
    syncCheckboxField(checkboxContractionEntry, data.auto_contraction_enabled);
    syncCheckboxField(checkboxSpreadExit, data.auto_spread_exit_enabled);
    
    if (document.activeElement !== paperModeInput) {
        paperModeInput.checked = data.paper_trading_mode;
    }
    if (document.activeElement !== autoTradingInput && data.auto_trading_enabled !== undefined) {
        autoTradingInput.checked = data.auto_trading_enabled;
    }
    
    if (!selectBroker.dataset.isDirty && document.activeElement !== selectBroker && data.broker !== undefined) {
        selectBroker.value = data.broker;
        if (data.broker === "AngelOne") {
            if (angeloneFields) angeloneFields.style.display = "grid";
            if (growwFields) growwFields.style.display = "none";
        } else if (data.broker === "Groww") {
            if (angeloneFields) angeloneFields.style.display = "none";
            if (growwFields) growwFields.style.display = "grid";
        } else {
            if (angeloneFields) angeloneFields.style.display = "none";
            if (growwFields) growwFields.style.display = "none";
        }
    }
    syncInputField(angeloneClientId, data.client_id);
    syncInputField(angelonePassword, data.password);
    syncInputField(angeloneTotp, data.totp_secret);
    syncInputField(angeloneApiKey, data.api_key);
    syncInputField(angelonePetalSymbol, data.petal_symbol);
    syncInputField(angelonePetalToken, data.petal_token);
    syncInputField(angeloneMiniSymbol, data.mini_symbol);
    syncInputField(angeloneMiniToken, data.mini_token);

    syncInputField(growwClientId, data.groww_client_id);
    syncInputField(growwApiKey, data.groww_api_key);
    syncInputField(growwSecret, data.groww_secret);
    syncInputField(growwPetalSymbol, data.groww_petal_symbol);
    syncInputField(growwMiniSymbol, data.groww_mini_symbol);

    // Render dynamic depth-based spreads
    const depthBuySpreadEl = document.getElementById("depth-buy-spread");
    const depthSellSpreadEl = document.getElementById("depth-sell-spread");
    if (depthBuySpreadEl && data.depth_buy_spread !== undefined && !isOffline) {
        depthBuySpreadEl.innerText = data.depth_buy_spread.toLocaleString('en-IN', { minimumFractionDigits: 2 });
    } else if (depthBuySpreadEl) {
        depthBuySpreadEl.innerText = "--";
    }
    if (depthSellSpreadEl && data.depth_sell_spread !== undefined && !isOffline) {
        depthSellSpreadEl.innerText = data.depth_sell_spread.toLocaleString('en-IN', { minimumFractionDigits: 2 });
    } else if (depthSellSpreadEl) {
        depthSellSpreadEl.innerText = "--";
    }

    // 8. Manual Trades Table
    const manualTrades = data.manual_trades || [];
    if (!manualTradesBody) {
        // Guard if element is missing
    } else if (manualTrades.length === 0) {
        manualTradesBody.innerHTML = `<tr><td colspan="9" class="empty-table">No active or pending manual trades.</td></tr>`;
    } else {
        manualTradesBody.innerHTML = "";
        manualTrades.forEach(trade => {
            const tr = document.createElement("tr");
            
            // Format status badge
            let statusBadge = "";
            const status = trade.status || "Pending";
            if (status === "Pending") {
                statusBadge = `<span class="badge-pending" style="background-color: rgba(245,158,11,0.15); color: #f59e0b; padding: 2px 6px; border-radius: 4px; font-size: 0.65rem; font-weight: 700; border: 1px solid rgba(245,158,11,0.25);">PENDING</span>`;
            } else if (status === "Open") {
                statusBadge = `<span class="badge-open" style="background-color: rgba(52,211,153,0.15); color: #34d399; padding: 2px 6px; border-radius: 4px; font-size: 0.65rem; font-weight: 700; border: 1px solid rgba(52,211,153,0.25);">OPEN</span>`;
            } else if (status === "Failed") {
                statusBadge = `<span class="badge-failed" style="background-color: rgba(239,68,68,0.15); color: #ef4444; padding: 2px 6px; border-radius: 4px; font-size: 0.65rem; font-weight: 700; border: 1px solid rgba(239,68,68,0.25);">FAILED</span>`;
            } else {
                statusBadge = `<span class="badge-closed" style="background-color: rgba(148,163,184,0.15); color: #94a3b8; padding: 2px 6px; border-radius: 4px; font-size: 0.65rem; font-weight: 700; border: 1px solid rgba(148,163,184,0.25);">${status.toUpperCase()}</span>`;
            }
            
            const triggerDiffDisplay = (trade.trigger_diff !== null && trade.trigger_diff !== undefined) ? parseFloat(trade.trigger_diff).toFixed(2) : "Immediate";
            const filledSpreadDisplay = (trade.entry_spread !== undefined && trade.entry_spread !== null && trade.entry_spread !== 0.0) ? parseFloat(trade.entry_spread).toFixed(2) : "--";
            const entrySlippage = trade.entry_slippage !== undefined ? trade.entry_slippage : 0.0;
            const slippageClass = entrySlippage > 0 ? "pnl-loss" : entrySlippage < 0 ? "pnl-profit" : "";
            const slippageText = (entrySlippage !== 0.0 && status !== "Pending") ? `<small class="${slippageClass}">(${(entrySlippage >= 0 ? "+" : "")}${entrySlippage.toFixed(1)})</small>` : "";

            let triggerColContent = "";
            if (status === "Pending") {
                triggerColContent = `<span class="font-mono"><strong>${triggerDiffDisplay}</strong></span>`;
            } else {
                triggerColContent = `
                    <div style="font-size: 0.72rem; line-height: 1.4;">
                        <div>Target: <span class="font-mono"><strong>${triggerDiffDisplay}</strong></span></div>
                        <div>Filled: <span class="font-mono"><strong>${filledSpreadDisplay}</strong></span> ${slippageText}</div>
                    </div>
                `;
            }

            const petalEntry = trade.petal_entry_price || 0.0;
            const miniEntry = trade.mini_entry_price || 0.0;
            
            // Format Col: Execution Prices
            let pricesContent = "--";
            if (status === "Open" || status === "Closed" || status === "Completed") {
                pricesContent = `
                    <div style="font-size: 0.72rem; line-height: 1.4;">
                        <div>P: <strong>${petalEntry.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</strong></div>
                        <div>M: <strong>${miniEntry.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</strong></div>
                    </div>
                `;
            }
            
            // Format Col: Live P&L
            let pnlContent = "--";
            if (status === "Open") {
                const unrealizedPnl = trade.unrealized_pnl || 0.0;
                const pnlClass = unrealizedPnl >= 0 ? "pnl-profit" : "pnl-loss";
                pnlContent = `<strong class="${pnlClass}" style="font-family: var(--font-mono);">${unrealizedPnl >= 0 ? "+" : ""}${unrealizedPnl.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</strong>`;
            } else if (status === "Closed" || status === "Completed") {
                const realizedPnl = trade.pnl || 0.0;
                const pnlClass = realizedPnl >= 0 ? "pnl-profit" : "pnl-loss";
                pnlContent = `<span class="${pnlClass}" style="font-family: var(--font-mono);">${realizedPnl >= 0 ? "+" : ""}${realizedPnl.toLocaleString('en-IN', { minimumFractionDigits: 2 })} (Net)</span>`;
            } else if (status === "Failed") {
                pnlContent = `<span style="color: var(--text-muted); font-size: 0.65rem; max-width: 150px; display: inline-block; word-break: break-word;">${trade.reason || "Trigger failed"}</span>`;
            }
            
            // Action button
            let actionBtn = "";
            if (status === "Pending") {
                actionBtn = `<button class="action-btn exit-button" onclick="cancelManualTrade(${trade.id})" style="padding: 0.2rem 0.5rem; font-size: 0.65rem; min-height: unset; margin: 0; background: #ea580c;">Cancel</button>`;
            } else if (status === "Open") {
                actionBtn = `<button class="action-btn exit-button" onclick="exitManualTrade(${trade.id})" style="padding: 0.2rem 0.5rem; font-size: 0.65rem; min-height: unset; margin: 0;">Square Off</button>`;
            } else {
                actionBtn = `<button class="metallic-button" onclick="dismissManualTrade(${trade.id})" style="padding: 0.2rem 0.5rem; font-size: 0.65rem; min-height: unset; margin: 0; background: rgba(255,255,255,0.03); color: var(--text-muted);">Dismiss</button>`;
            }
            
            const tradeTime = trade.entry_time || "--";
            const petalSym = trade.petal_symbol || data.petal_symbol || "GOLDPETAL";
            const miniSym = trade.mini_symbol || data.mini_symbol || "GOLDMINI";
            const symbolsContent = `
                <div style="font-size: 0.7rem; line-height: 1.3;">
                    <div>L1: <strong class="font-mono" style="color: var(--text-primary);">${petalSym}</strong></div>
                    <div>L2: <strong class="font-mono" style="color: var(--text-secondary);">${miniSym}</strong></div>
                </div>
            `;
            
            tr.innerHTML = `
                <td class="font-mono">${trade.id}</td>
                <td>${tradeTime}</td>
                <td>${symbolsContent}</td>
                <td><strong>${trade.direction}</strong></td>
                <td class="font-mono">${trade.quantity}</td>
                <td>${triggerColContent}</td>
                <td>${statusBadge}</td>
                <td>${pricesContent}</td>
                <td>${pnlContent}</td>
                <td style="text-align: right; padding-right: 1.5rem;">${actionBtn}</td>
            `;
            
            manualTradesBody.appendChild(tr);
        });
    }

    // 7. Trade History Table
    if (data.trade_history.length === 0) {
        historyBody.innerHTML = `<tr><td colspan="7" class="empty-table">No completed or cancelled trades yet.</td></tr>`;
    } else {
        historyBody.innerHTML = "";
        data.trade_history.forEach(trade => {
            const tr = document.createElement("tr");
            const status = trade.status || "COMPLETED";
            
            let statusBadge = "";
            if (status === "COMPLETED") {
                statusBadge = `<span class="badge-completed" style="background-color: rgba(52,211,153,0.15); color: #34d399; padding: 2px 6px; border-radius: 4px; font-size: 0.65rem; font-weight: 700; border: 1px solid rgba(52,211,153,0.25);">COMPLETED</span>`;
            } else if (status === "CANCELLED") {
                statusBadge = `<span class="badge-cancelled" style="background-color: rgba(245,158,11,0.15); color: #f59e0b; padding: 2px 6px; border-radius: 4px; font-size: 0.65rem; font-weight: 700; border: 1px solid rgba(245,158,11,0.25);">CANCELLED</span>`;
            } else {
                statusBadge = `<span class="badge-failed" style="background-color: rgba(239,68,68,0.15); color: #ef4444; padding: 2px 6px; border-radius: 4px; font-size: 0.65rem; font-weight: 700; border: 1px solid rgba(239,68,68,0.25);">FAILED</span>`;
            }
            
            const petalEntry = trade.petal_entry || 0.0;
            const miniEntry = trade.mini_entry || 0.0;
            const petalExit = trade.petal_exit || 0.0;
            const miniExit = trade.mini_exit || 0.0;
            
            const entrySlippage = trade.entry_slippage || 0.0;
            const exitSlippage = trade.exit_slippage || 0.0;
            const actualEntry = trade.actual_entry_spread || 0.0;
            const actualExit = trade.actual_exit_spread || 0.0;
            
            const date = trade.date || "";
            const entryTime = trade.entry_time || "--";
            const exitTime = trade.exit_time || "--";
            const timeRange = `${entryTime} ➔ ${exitTime}`;
            
            // Format Col 4: Execution Prices (Petal / Mini)
            let pricesContent = "";
            if (status === "COMPLETED") {
                pricesContent = `
                    <div style="font-size: 0.72rem; line-height: 1.4;">
                        <div><span style="color: var(--text-muted); font-size: 0.65rem;">Entry:</span> P: <strong>${petalEntry.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</strong> / M: <strong>${miniEntry.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</strong></div>
                        <div><span style="color: var(--text-muted); font-size: 0.65rem;">Exit:</span> P: <strong>${petalExit.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</strong> / M: <strong>${miniExit.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</strong></div>
                    </div>
                `;
            } else {
                pricesContent = `<span class="text-muted">--</span>`;
            }

            // Format Col 5: Spreads & Slippage
            let spreadsContent = "";
            if (status === "COMPLETED") {
                const entrySlippageClass = entrySlippage > 0 ? "pnl-loss" : entrySlippage < 0 ? "pnl-profit" : "";
                const exitSlippageClass = exitSlippage > 0 ? "pnl-loss" : exitSlippage < 0 ? "pnl-profit" : "";
                spreadsContent = `
                    <div style="font-size: 0.72rem; line-height: 1.4;">
                        <div><span style="color: var(--text-muted); font-size: 0.65rem;">Entry:</span> <strong>${actualEntry.toFixed(0)}</strong> <small class="${entrySlippageClass}">${(entrySlippage >= 0 ? "+" : "")}${entrySlippage.toFixed(1)}</small></div>
                        <div><span style="color: var(--text-muted); font-size: 0.65rem;">Exit:</span> <strong>${actualExit.toFixed(0)}</strong> <small class="${exitSlippageClass}">${(exitSlippage >= 0 ? "+" : "")}${exitSlippage.toFixed(1)}</small></div>
                    </div>
                `;
            } else {
                spreadsContent = `<span class="text-muted">--</span>`;
            }
            
            const grossPnl = trade.gross_pnl !== undefined ? trade.gross_pnl : trade.pnl || 0.0;
            const charges = trade.charges || 0.0;
            const netPnl = trade.pnl || 0.0;
            
            const grossSign = grossPnl >= 0 ? "+" : "";
            const netSign = netPnl >= 0 ? "+" : "";
            
            let pnlCellContent = "";
            if (status === "COMPLETED") {
                pnlCellContent = `
                    <div style="text-align: right; display: inline-block; width: 100%;">
                        <strong class="${netPnl >= 0 ? 'pnl-profit' : 'pnl-loss'}" style="font-size: 0.85rem;">${netSign}${netPnl.toFixed(2)}</strong>
                        <div style="font-size: 0.62rem; color: var(--text-secondary); margin-top: 0.15rem; white-space: nowrap;">
                            Gross: ${grossSign}${grossPnl.toFixed(2)} | Fees: ${charges.toFixed(2)}
                        </div>
                    </div>
                `;
            } else {
                pnlCellContent = `<span class="text-muted">--</span>`;
            }
            
            tr.innerHTML = `
                <td>${trade.id}</td>
                <td style="font-size: 0.72rem; color: var(--text-secondary); white-space: nowrap;"><strong>${date}</strong><br><small class="text-muted">${timeRange}</small></td>
                <td>
                    <div style="font-weight: 700; font-size: 0.75rem; margin-bottom: 2px;">${trade.direction}</div>
                    ${statusBadge}
                </td>
                <td class="font-mono">${pricesContent}</td>
                <td class="font-mono">${spreadsContent}</td>
                <td class="font-mono" style="text-align: right; padding-right: 1.5rem;">${pnlCellContent}</td>
                <td style="font-size: 0.7rem; color: var(--text-secondary); max-width: 220px; white-space: normal; line-height: 1.3;">
                    <strong>${trade.reason || "--"}</strong><br>
                    <small class="text-muted">${trade.details || ""}</small>
                </td>
            `;
            historyBody.appendChild(tr);
        });
    }

    // 8. Console logs terminal update
    logsContainer.innerHTML = "";
    data.logs.forEach(log => {
        const line = document.createElement("div");
        line.className = "log-line";

        if (log.includes("Stop-Loss") || log.includes("KILL-SWITCH") || log.includes("EMERGENCY") || log.includes("Stop Loss Triggered")) {
            line.classList.add("trade-halt");
        } else if (log.includes("SQUARED OFF") || log.includes("Profit booked") || log.includes("CLOSED")) {
            line.classList.add("trade-exit");
        } else if (log.includes("ENTRY") || log.includes("Position entered") || log.includes("Entered")) {
            line.classList.add("trade-entry");
        } else {
            line.classList.add("system-msg");
        }

        line.innerText = log;
        logsContainer.appendChild(line);
    });
    logsContainer.scrollTop = logsContainer.scrollHeight;
    
    // Update Bot Execution Timeline Pipeline
    updatePipelineTimeline(data);
}

// Element class flasher utility helper
function flashElement(element, className) {
    element.classList.add(className);
    setTimeout(() => element.classList.remove(className), 600);
}

// Field synchronizers preventing cursor focus conflicts and reverting values
function syncInputField(element, value) {
    if (value === undefined || value === null || !element) return;
    
    const serverValStr = String(value);
    const currentValStr = element.value;
    const lastSyncedVal = element.dataset.lastSynced;
    
    // Case 1: The input value is already equal to the server value.
    if (currentValStr === serverValStr) {
        element.dataset.lastSynced = serverValStr;
        return;
    }
    
    // Case 2: The user has not edited the input locally (still equals last synced value),
    // or it is the first sync. Update input value.
    if (lastSyncedVal === undefined || currentValStr === lastSyncedVal) {
        if (document.activeElement !== element) {
            element.value = serverValStr;
            element.dataset.lastSynced = serverValStr;
        }
    }
}

function syncCheckboxField(element, checked) {
    if (checked === undefined || checked === null || !element) return;
    
    const serverVal = !!checked;
    const currentVal = element.checked;
    const lastSyncedVal = element.dataset.lastSynced === "true";
    const hasLastSynced = element.dataset.lastSynced !== undefined;
    
    // Case 1: Already matches the server state.
    if (currentVal === serverVal) {
        element.dataset.lastSynced = String(serverVal);
        return;
    }
    
    // Case 2: Not edited locally since last sync, or first sync.
    if (!hasLastSynced || currentVal === lastSyncedVal) {
        if (document.activeElement !== element) {
            element.checked = serverVal;
            element.dataset.lastSynced = String(serverVal);
        }
    }
}

// API Post requests dispatcher
function postAction(endpoint, payload = {}) {
    const httpProtocol = window.location.protocol;
    const url = `${httpProtocol}//${host}/api/${endpoint}?token=${AUTH_TOKEN}`;
    
    return fetch(url, {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: Object.keys(payload).length > 0 ? JSON.stringify(payload) : null
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.json();
    })
    .catch(error => {
        console.error(`Error executing ${endpoint}:`, error);
        logLocalMessage(`[SYSTEM] API Action failed: ${error.message}`);
    });
}

// Handle manual Position Entry action
entryBtn.addEventListener("click", () => {
    const selectedDirEl = document.querySelector('input[name="trade-direction"]:checked');
    if (!selectedDirEl) return;
    
    const direction = selectedDirEl.value;
    const diffValRaw = manualTriggerDiff ? manualTriggerDiff.value.trim() : "";
    const triggerDiff = diffValRaw !== "" ? parseFloat(diffValRaw) : null;
    
    if (triggerDiff !== null && isNaN(triggerDiff)) {
        alert("Please enter a valid trigger difference value, or leave it empty.");
        return;
    }
    
    if (triggerDiff !== null) {
        logLocalMessage(`[SYSTEM] Dispatching manual ${direction} entry command with trigger difference ${triggerDiff}...`);
        postAction("entry", { direction: direction, trigger_diff: triggerDiff });
        if (manualTriggerDiff) manualTriggerDiff.value = "";
    } else {
        logLocalMessage(`[SYSTEM] Dispatching manual ${direction} entry command...`);
        postAction("entry", { direction: direction });
    }
});

// Handle manual Position Exit square-off action
exitBtn.addEventListener("click", () => {
    logLocalMessage("[SYSTEM] Dispatching manual square-off command...");
    postAction("exit");
});

// Handle Emergency Kill Switch action
killSwitchBtn.addEventListener("click", () => {
    if (confirm("WARNING: Are you sure you want to trigger the EMERGENCY KILL SWITCH? This will square-off all active positions and halt trading.")) {
        logLocalMessage("[SYSTEM] TRIGGERING EMERGENCY TERMINAL SHUTDOWN...");
        postAction("kill-switch");
    }
});

// Window-level manual trade action helper functions
window.cancelManualTrade = function(tradeId) {
    if (confirm(`Are you sure you want to cancel pending manual trade ID ${tradeId}?`)) {
        logLocalMessage(`[SYSTEM] Cancelling pending manual trade ID ${tradeId}...`);
        postAction("exit-manual", { trade_id: tradeId });
    }
};

window.exitManualTrade = function(tradeId) {
    if (confirm(`Are you sure you want to square off manual trade ID ${tradeId}?`)) {
        logLocalMessage(`[SYSTEM] Squaring off manual trade ID ${tradeId}...`);
        postAction("exit-manual", { trade_id: tradeId });
    }
};

window.dismissManualTrade = function(tradeId) {
    postAction("dismiss-manual", { trade_id: tradeId });
};

// Submit strategy rules & config form parameters to the backend
function saveParameters() {
    const entryVal = parseFloat(entryInput.value);
    const targetVal = parseFloat(targetInput.value);
    const slVal = parseFloat(slInput.value);
    const totalCapitalVal = parseFloat(capitalInput.value);
    
    const autoTarget = checkboxTarget.checked;
    const autoTargetVal = parseFloat(inputAutoTargetVal.value);
    
    const autoSl = checkboxSl.checked;
    const autoSlVal = parseFloat(inputAutoSlVal.value);
    
    const autoSquareoff = checkboxSquareoff.checked;
    const autoSquareoffTime = inputAutoSquareoffTime.value.trim();
    const paperMode = paperModeInput.checked;
    const autoTrading = autoTradingInput.checked;
    
    const spreadBufferVal = parseFloat(inputSpreadBuffer.value) || 0.0;
    const autoContraction = checkboxContractionEntry.checked;
    const autoSpreadExit = checkboxSpreadExit.checked;

    // Broker configs
    const broker = selectBroker.value;
    const clientIdVal = angeloneClientId.value.trim();
    const passwordVal = angelonePassword.value.trim();
    const totpVal = angeloneTotp.value.trim();
    const apiKeyVal = angeloneApiKey.value.trim();
    const petalSymbol = angelonePetalSymbol ? angelonePetalSymbol.value.trim() : "";
    const petalToken = angelonePetalToken ? angelonePetalToken.value.trim() : "";
    const miniSymbol = angeloneMiniSymbol ? angeloneMiniSymbol.value.trim() : "";
    const miniToken = angeloneMiniToken ? angeloneMiniToken.value.trim() : "";

    const growwClientIdVal = growwClientId ? growwClientId.value.trim() : "";
    const growwApiKeyVal = growwApiKey ? growwApiKey.value.trim() : "";
    const growwSecretVal = growwSecret ? growwSecret.value.trim() : "";
    const growwPetalSymbolVal = growwPetalSymbol ? growwPetalSymbol.value.trim() : "";
    const growwMiniSymbolVal = growwMiniSymbol ? growwMiniSymbol.value.trim() : "";

    const tradeQtyVal = parseInt(quantityInput.value) || 1;

    if (isNaN(entryVal) || isNaN(targetVal) || isNaN(slVal) || isNaN(autoTargetVal) || isNaN(autoSlVal) || isNaN(totalCapitalVal) || isNaN(tradeQtyVal) || isNaN(spreadBufferVal)) {
        logLocalMessage("[SYSTEM] Error: Numeric config fields must hold valid values.");
        return;
    }
    
    const timeRegex = /^([01]?[0-9]|2[0-3]):[0-5][0-9]$/;
    if (autoSquareoff && !timeRegex.test(autoSquareoffTime)) {
        logLocalMessage("[SYSTEM] Error: Auto square-off time must match HH:MM format (24-hour).");
        return;
    }

    logLocalMessage("[SYSTEM] Syncing configurations with backend...");
    postAction("update-rules", {
        entry_threshold: entryVal,
        target_threshold: targetVal,
        stop_loss_threshold: slVal,
        total_capital: totalCapitalVal,
        paper_trading_mode: paperMode,
        trade_quantity: tradeQtyVal,
        auto_target_enabled: autoTarget,
        auto_target_val: autoTargetVal,
        auto_sl_enabled: autoSl,
        auto_sl_val: autoSlVal,
        auto_square_off_enabled: autoSquareoff,
        auto_square_off_time: autoSquareoffTime,
        auto_trading_enabled: autoTrading,
        spread_buffer: spreadBufferVal,
        auto_contraction_enabled: autoContraction,
        auto_spread_exit_enabled: autoSpreadExit,
        broker: broker,
        api_key: apiKeyVal,
        client_id: clientIdVal,
        password: passwordVal,
        totp_secret: totpVal,
        petal_symbol: petalSymbol,
        petal_token: petalToken,
        mini_symbol: miniSymbol,
        mini_token: miniToken,
        groww_client_id: growwClientIdVal,
        groww_api_key: growwApiKeyVal,
        groww_secret: growwSecretVal,
        groww_petal_symbol: growwPetalSymbolVal,
        groww_mini_symbol: growwMiniSymbolVal
    })
    .then(data => {
        if (data && data.status === "SUCCESS") {
            // Clear dirty flags
            inputsToTrack.forEach(input => { if (input) delete input.dataset.isDirty; });
            checkboxesToTrack.forEach(cb => { if (cb) delete cb.dataset.isDirty; });
            if (selectBroker) delete selectBroker.dataset.isDirty;
            
            const mode = paperMode ? "Paper Mode" : "Live Mode";
            const autoState = autoTrading ? "ON" : "OFF";
            logLocalMessage(`[SYSTEM] System updated. Capital: ${totalCapitalVal.toLocaleString()} INR. Broker: ${broker}. Mode: ${mode}.`);
        }
    });
}

// Track user modifications (dirty fields)
const inputsToTrack = [
    entryInput, targetInput, slInput, capitalInput, quantityInput,
    inputSpreadBuffer, inputAutoTargetVal, inputAutoSlVal, inputAutoSquareoffTime,
    growwClientId, growwApiKey, growwSecret, growwPetalSymbol, growwMiniSymbol,
    angeloneClientId, angelonePassword, angeloneTotp, angeloneApiKey, angelonePetalSymbol, angelonePetalToken, angeloneMiniSymbol, angeloneMiniToken
];
inputsToTrack.forEach(input => {
    if (input) {
        input.addEventListener("input", () => {
            input.dataset.isDirty = "true";
        });
    }
});

const checkboxesToTrack = [
    checkboxTarget, checkboxSl, checkboxSquareoff, checkboxContractionEntry, checkboxSpreadExit
];
checkboxesToTrack.forEach(cb => {
    if (cb) {
        cb.addEventListener("change", () => {
            cb.dataset.isDirty = "true";
        });
    }
});

// Event listener bindings for parameter saves and mode toggles
updateParamsBtn.addEventListener("click", saveParameters);
paperModeInput.addEventListener("change", saveParameters);
autoTradingInput.addEventListener("change", saveParameters);
checkboxTarget.addEventListener("change", saveParameters);
checkboxSl.addEventListener("change", saveParameters);
checkboxSquareoff.addEventListener("change", saveParameters);
checkboxContractionEntry.addEventListener("change", saveParameters);
checkboxSpreadExit.addEventListener("change", saveParameters);
inputSpreadBuffer.addEventListener("change", saveParameters);

selectBroker.addEventListener("change", () => {
    selectBroker.dataset.isDirty = "true";
    if (selectBroker.value === "AngelOne") {
        if (angeloneFields) angeloneFields.style.display = "grid";
        if (growwFields) growwFields.style.display = "none";
    } else if (selectBroker.value === "Groww") {
        if (angeloneFields) angeloneFields.style.display = "none";
        if (growwFields) growwFields.style.display = "grid";
    } else {
        if (angeloneFields) angeloneFields.style.display = "none";
        if (growwFields) growwFields.style.display = "none";
    }
});

// Handle Strategy rules JSON download
downloadBtn.addEventListener("click", () => {
    logLocalMessage("[SYSTEM] Downloading strategy parameters file...");
    const httpProtocol = window.location.protocol;
    const url = `${httpProtocol}//${host}/api/download-logic?token=${AUTH_TOKEN}`;
    
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = "strategy_rules.json";
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
});

// Handle Trade History CSV Export
exportCsvBtn.addEventListener("click", () => {
    logLocalMessage("[SYSTEM] Exporting trade history records to CSV...");
    const httpProtocol = window.location.protocol;
    const url = `${httpProtocol}//${host}/api/export-csv?token=${AUTH_TOKEN}`;
    
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = "trade_history.csv";
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    logLocalMessage("[SYSTEM] Trade history CSV downloaded.");
});

// Handle Active & Pending Manual Trades CSV Export
if (exportManualCsvBtn) {
    exportManualCsvBtn.addEventListener("click", () => {
        logLocalMessage("[SYSTEM] Exporting active & pending manual trades to CSV...");
        const httpProtocol = window.location.protocol;
        const url = `${httpProtocol}//${host}/api/export-manual-csv?token=${AUTH_TOKEN}`;
        
        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = "manual_trades.csv";
        document.body.appendChild(anchor);
        anchor.click();
        document.body.removeChild(anchor);
        logLocalMessage("[SYSTEM] Manual trades CSV downloaded.");
    });
}

// Run Initial WebSocket Connection
connect();

const toggleHelpBtn = document.getElementById("toggle-help-btn");
const helpContent = document.getElementById("help-content");
const helpArrow = document.getElementById("help-arrow");

if (toggleHelpBtn && helpContent && helpArrow) {
    toggleHelpBtn.addEventListener("click", () => {
        if (helpContent.style.display === "none" || helpContent.style.display === "") {
            helpContent.style.display = "block";
            helpArrow.innerText = "▲";
        } else {
            helpContent.style.display = "none";
            helpArrow.innerText = "▼";
        }
    });
}

// Update Bot Execution Pipeline Steps
function updatePipelineTimeline(data) {
    const stepScan = document.getElementById("wf-step-scan");
    const stepLiquidity = document.getElementById("wf-step-liquidity");
    const stepOrder1 = document.getElementById("wf-step-order1");
    const stepOrder2 = document.getElementById("wf-step-order2");
    const stepPosition = document.getElementById("wf-step-position");
    
    const labelScan = stepScan ? stepScan.querySelector(".wf-label") : null;
    const labelLiquidity = stepLiquidity ? stepLiquidity.querySelector(".wf-label") : null;
    const labelOrder1 = stepOrder1 ? stepOrder1.querySelector(".wf-label") : null;
    const labelOrder2 = stepOrder2 ? stepOrder2.querySelector(".wf-label") : null;
    const labelPosition = stepPosition ? stepPosition.querySelector(".wf-label") : null;
    
    // Default Reset Helper
    function resetStep(stepEl, labelEl, defaultText) {
        if (stepEl) stepEl.className = "wf-step";
        if (labelEl) labelEl.innerText = defaultText;
    }
    
    const status = data.system_status; // "Active", "In-Position", "Halted"
    const spread = data.spread;
    const entryThresh = data.entry_threshold || 850;
    
    const isOffline = !data.api_connected && data.broker === "AngelOne" && !data.paper_trading_mode;
    if (isOffline) {
        if (stepScan) stepScan.className = "wf-step failed";
        if (labelScan) labelScan.innerText = "1. Broker Offline";
        resetStep(stepLiquidity, labelLiquidity, "2. Depth Check");
        resetStep(stepOrder1, labelOrder1, "3. Order Leg 1");
        resetStep(stepOrder2, labelOrder2, "4. Order Leg 2");
        resetStep(stepPosition, labelPosition, "5. Holding Position");
        return;
    }
    
    if (status === "Halted") {
        if (stepScan) stepScan.className = "wf-step failed";
        if (labelScan) labelScan.innerText = "1. Bot Halted";
        resetStep(stepLiquidity, labelLiquidity, "2. Depth Check");
        resetStep(stepOrder1, labelOrder1, "3. Order Leg 1");
        resetStep(stepOrder2, labelOrder2, "4. Order Leg 2");
        resetStep(stepPosition, labelPosition, "5. Holding Position");
        return;
    }
    
    // Scan Logs for recent Liquidity shield failure or order failures
    const logs = data.logs || [];
    let hasLiquidityFail = false;
    let lastLog = "";
    if (logs.length > 0) {
        lastLog = logs[logs.length - 1];
        if (lastLog.includes("LIQUIDITY SHIELD") || lastLog.includes("insufficient liquidity") || lastLog.includes("Insufficient depth") || lastLog.includes("Bid-Ask gap too wide")) {
            hasLiquidityFail = true;
        }
    }
    
    if (data.is_in_position) {
        // Active holding position
        if (stepScan) {
            stepScan.className = "wf-step complete";
            if (labelScan) labelScan.innerText = "1. Scan Complete";
        }
        if (stepLiquidity) {
            stepLiquidity.className = "wf-step complete";
            if (labelLiquidity) labelLiquidity.innerText = "2. Depth Checked";
        }
        if (stepOrder1) {
            stepOrder1.className = "wf-step complete";
            if (labelOrder1) labelOrder1.innerText = `3. Leg 1 Filled @ ${data.petal_entry_price}`;
        }
        if (stepOrder2) {
            stepOrder2.className = "wf-step complete";
            if (labelOrder2) labelOrder2.innerText = `4. Leg 2 Filled @ ${data.mini_entry_price}`;
        }
        if (stepPosition) {
            stepPosition.className = "wf-step active";
            if (labelPosition) labelPosition.innerText = `5. Holding: ${data.position_direction}`;
        }
    } else {
        // Not in position (Scanning for entry)
        if (stepScan) {
            stepScan.className = "wf-step active";
            if (labelScan) labelScan.innerText = `1. Scanning Spread (${Math.round(spread)})`;
        }
        
        if (hasLiquidityFail) {
            if (stepLiquidity) {
                stepLiquidity.className = "wf-step failed";
                if (labelLiquidity) labelLiquidity.innerText = "2. Depth Gap Wide/Low";
            }
            resetStep(stepOrder1, labelOrder1, "3. Order Leg 1 (Skipped)");
            resetStep(stepOrder2, labelOrder2, "4. Order Leg 2 (Skipped)");
            resetStep(stepPosition, labelPosition, "5. Holding Position");
        } else {
            // Check transitional state logs
            const isPlacing = lastLog.includes("Placing parallel orders") || lastLog.includes("Dispatching parallel orders") || lastLog.includes("Placing sequential orders");
            const isRollback = lastLog.includes("EMERGENCY ROLLBACK") || lastLog.includes("Reversing Leg");
            
            if (isPlacing) {
                if (stepScan) {
                    stepScan.className = "wf-step complete";
                    if (labelScan) labelScan.innerText = "1. Spread Triggered";
                }
                if (stepLiquidity) {
                    stepLiquidity.className = "wf-step complete";
                    if (labelLiquidity) labelLiquidity.innerText = "2. Depth Checked";
                }
                if (stepOrder1) {
                    stepOrder1.className = "wf-step active";
                    if (labelOrder1) labelOrder1.innerText = "3. Executing Leg 1...";
                }
                if (stepOrder2) {
                    stepOrder2.className = "wf-step active";
                    if (labelOrder2) labelOrder2.innerText = "4. Executing Leg 2...";
                }
                resetStep(stepPosition, labelPosition, "5. Holding Position");
            } else if (isRollback) {
                if (stepScan) {
                    stepScan.className = "wf-step complete";
                    if (labelScan) labelScan.innerText = "1. Spread Triggered";
                }
                if (stepLiquidity) {
                    stepLiquidity.className = "wf-step complete";
                    if (labelLiquidity) labelLiquidity.innerText = "2. Depth Checked";
                }
                if (stepOrder1) {
                    stepOrder1.className = "wf-step failed";
                    if (labelOrder1) labelOrder1.innerText = "3. Partial Fill / Failed";
                }
                if (stepOrder2) {
                    stepOrder2.className = "wf-step failed";
                    if (labelOrder2) labelOrder2.innerText = "4. Emergency Rollback";
                }
                resetStep(stepPosition, labelPosition, "5. Holding Position");
            } else {
                if (stepLiquidity) {
                    stepLiquidity.className = "wf-step";
                    if (labelLiquidity) labelLiquidity.innerText = "2. Depth Check (Ready)";
                }
                resetStep(stepOrder1, labelOrder1, "3. Order Leg 1");
                resetStep(stepOrder2, labelOrder2, "4. Order Leg 2");
                resetStep(stepPosition, labelPosition, "5. Holding Position");
            }
        }
    }
}

// Render 5-Level orderbook depth rows
function renderOrderBook(prefix, depthData) {
    const container = document.getElementById(`${prefix}-ob-rows`);
    if (!container) return;
    
    const buyList = depthData ? depthData.buy || [] : [];
    const sellList = depthData ? depthData.sell || [] : [];
    
    if (buyList.length === 0 && sellList.length === 0) {
        container.innerHTML = `<div class="ob-row-placeholder">No depth data available</div>`;
        return;
    }
    
    let html = "";
    for (let i = 0; i < 5; i++) {
        const bid = buyList[i] || { quantity: 0, orders: 0, price: 0.0 };
        const ask = sellList[i] || { quantity: 0, orders: 0, price: 0.0 };
        
        const bidQty = bid.quantity > 0 ? bid.quantity.toLocaleString() : "--";
        const bidOrders = bid.orders > 0 ? bid.orders : "--";
        const bidPrice = bid.price > 0 ? bid.price.toLocaleString('en-IN', { maximumFractionDigits: 0 }) : "--";
        
        const askPrice = ask.price > 0 ? ask.price.toLocaleString('en-IN', { maximumFractionDigits: 0 }) : "--";
        const askOrders = ask.orders > 0 ? ask.orders : "--";
        const askQty = ask.quantity > 0 ? ask.quantity.toLocaleString() : "--";
        
        html += `
            <div class="ob-row">
                <span class="ob-cell-bid-qty">${bidQty}</span>
                <span class="ob-cell-bid-orders">${bidOrders}</span>
                <span class="ob-cell-bid-price">${bidPrice}</span>
                <span class="ob-cell-ask-price">${askPrice}</span>
                <span class="ob-cell-ask-orders">${askOrders}</span>
                <span class="ob-cell-ask-qty">${askQty}</span>
            </div>
        `;
    }
    container.innerHTML = html;
}

// Update Top-Level Workflow Pipeline flowchart
function updateTopWorkflowBanner(data) {
    const stepScan = document.getElementById("wf-step-scan");
    const stepLiquidity = document.getElementById("wf-step-liquidity");
    const stepOrder1 = document.getElementById("wf-step-order1");
    const stepOrder2 = document.getElementById("wf-step-order2");
    const stepPosition = document.getElementById("wf-step-position");
    
    function setStepState(stepEl, state) {
        if (!stepEl) return;
        stepEl.className = "wf-step " + state;
    }
    
    function resetStep(stepEl, statusEl, defaultStatus) {
        if (stepEl) stepEl.className = "pipeline-step";
        if (statusEl) statusEl.innerText = defaultStatus;
    }
    
    const isOffline = !data.api_connected && data.broker === "AngelOne";
    if (isOffline) {
        setStepState(stepScan, "failed");
        setStepState(stepLiquidity, "");
        setStepState(stepOrder1, "");
        setStepState(stepOrder2, "");
        setStepState(stepPosition, "");
        
        const scanLabel = stepScan ? stepScan.querySelector(".wf-label") : null;
        if (scanLabel) scanLabel.innerText = "API Offline";
        return;
    } else {
        const scanLabel = stepScan ? stepScan.querySelector(".wf-label") : null;
        if (scanLabel) scanLabel.innerText = "1. Scanning Spread";
    }
    
    const status = data.system_status; // "Active", "In-Position", "Halted"
    const logs = data.logs || [];
    let hasLiquidityFail = false;
    let lastLog = "";
    if (logs.length > 0) {
        lastLog = logs[logs.length - 1];
        if (lastLog.includes("LIQUIDITY SHIELD") || lastLog.includes("insufficient liquidity") || lastLog.includes("Insufficient depth")) {
            hasLiquidityFail = true;
        }
    }
    
    if (status === "Halted") {
        setStepState(stepScan, "failed");
        setStepState(stepLiquidity, "");
        setStepState(stepOrder1, "");
        setStepState(stepOrder2, "");
        setStepState(stepPosition, "");
        return;
    }
    
    if (data.is_in_position) {
        setStepState(stepScan, "complete");
        setStepState(stepLiquidity, "complete");
        setStepState(stepOrder1, "complete");
        setStepState(stepOrder2, "complete");
        setStepState(stepPosition, "active");
    } else {
        if (hasLiquidityFail) {
            setStepState(stepScan, "complete");
            setStepState(stepLiquidity, "failed");
            setStepState(stepOrder1, "");
            setStepState(stepOrder2, "");
            setStepState(stepPosition, "");
        } else {
            setStepState(stepScan, "active");
            setStepState(stepLiquidity, "");
            setStepState(stepOrder1, "");
            setStepState(stepOrder2, "");
            setStepState(stepPosition, "");
            
            if (lastLog.includes("Placing sequential orders") || lastLog.includes("Dispatching sequential orders")) {
                setStepState(stepScan, "complete");
                setStepState(stepLiquidity, "complete");
                setStepState(stepOrder1, "active");
            } else if (lastLog.includes("Leg 1: GOLDPETAL filled") || lastLog.includes("Leg 1 GOLDPETAL filled")) {
                setStepState(stepScan, "complete");
                setStepState(stepLiquidity, "complete");
                setStepState(stepOrder1, "complete");
                setStepState(stepOrder2, "active");
            }
        }
    }
}

// Global toggle handler for collapsible control/data cards
window.toggleCard = function(cardId) {
    const card = document.getElementById(cardId);
    if (!card) return;
    const content = card.querySelector(".card-content");
    const arrow = card.querySelector(".toggle-arrow");
    
    if (content.style.display === "none" || !content.style.display) {
        if (cardId === "card-logs") {
            content.style.display = "block";
        } else {
            content.style.display = "flex";
        }
        if (arrow) arrow.style.transform = "rotate(180deg)";
    } else {
        content.style.display = "none";
        if (arrow) arrow.style.transform = "rotate(0deg)";
    }
};
