import datetime
import pandas as pd
import numpy as np
from jqdata import *

# ==================== 全局配置 ====================
def initialize(context):
    """
    策略初始化 - 修改风控参数
    """
     # 基础设置
    set_benchmark('000300.XSHG')
    set_option('use_real_price', True)
    set_order_cost(OrderCost(open_tax=0, close_tax=0.0005, 
                            open_commission=0.0005, close_commission=0.0005, 
                            min_commission=5), type='stock')
    
    # 全局变量
    g.stock_list = []           # 基础股票池
    g.candidate_stocks = []     # 候选股票
    g.positions = {}            # 持仓信息
    g.trading_enabled = True    # 交易开关
    
    # 参数设置 - 只修改风控部分
    g.params = {
        # 基础筛选参数
        'min_turnover': 7,
        'max_turnover': 12,
        'min_price_change': 4,
        'max_market_cap': 100,
        
        # 形态参数
        'min_rise_pct': 30,
        'min_fall_pct': 25,
        'max_a_kill_days': 90,
        'min_a_kill_score': 50,
        'min_wave_score': 40,
        'max_total_days': 120,
        
        # =========== 优化后的风控参数 ===========
        'position_ratio': 0.1,           # 单只股票仓位20%
        'max_positions': 10,              # 最大持仓数量
        
        # 修改的核心参数：
        'immediate_stop_loss': 0.15,     # 立即止损：5%（原3%）→ 给更多缓冲
        'immediate_take_profit': 0.2,   # 立即止盈：12%（原8%）→ 让利润多跑
        'tail_take_profit': 0.15,        # 14:55止盈：6%（原4%）→ 提高目标
        'time_stop_days': 30,             # 时间止损：4天（原3天）→ 给更多时间
        # ======================================
    }
    
    # 运行计划（保持不变）
    run_daily(initialize_stock_pool, time='09:00')
    run_daily(morning_cleanup, time='09:25')
    run_daily(check_tail_position, time='14:55')
    run_daily(trade_logic, time='14:56')
    
    log.info("策略初始化完成 - 风控参数已优化")
# ==================== 股票池管理 ====================
def initialize_stock_pool(context):
    """初始化基础股票池（每天更新）"""
    try:
        all_stocks = get_all_securities(types='stock', date=context.previous_date)
        current_data = get_current_data()
        today = context.current_dt.date()
        new_stock_threshold = datetime.timedelta(days=365)
        
        filtered_stocks = [
            stock for stock in all_stocks.index
            if (today - all_stocks.loc[stock, 'start_date']) > new_stock_threshold
            and not current_data[stock].is_st
            and not stock.startswith('BJ')
            and not stock.startswith('68')
            and not stock.startswith('3')
        ]
        
        q = query(valuation.code).filter(
            valuation.market_cap < g.params['max_market_cap'], 
            valuation.code.in_(filtered_stocks)
        )
        df = get_fundamentals(q, date=context.previous_date)
        
        if df.empty:
            g.stock_list = []
            return
        
        g.stock_list = []
        for stock_code in df['code']:
            stock_name = all_stocks.loc[stock_code, 'display_name']
            g.stock_list.append((stock_code, stock_name))
        
        log.info(f"股票池更新: {len(g.stock_list)}只股票")
        
    except Exception as e:
        log.error(f"初始化股票池失败: {str(e)}")
        g.stock_list = []

# ==================== 股东户数变化因子  ====================
def get_shareholder_change(stock_code, current_date):
    """
    获取股东户数较上期的变化百分比
    返回：正数表示股东户数减少（筹码集中），负数表示增加，None表示数据不足
    """
    try:
        q = query(
            finance.STK_HOLDER_NUM.code,
            finance.STK_HOLDER_NUM.end_date,
            finance.STK_HOLDER_NUM.share_holders
        ).filter(
            finance.STK_HOLDER_NUM.code == stock_code,
            finance.STK_HOLDER_NUM.end_date <= current_date
        ).order_by(
            finance.STK_HOLDER_NUM.end_date.desc()
        ).limit(2)

        df = finance.run_query(q)

        if df is None or len(df) < 2:
            return None

        latest = df.iloc[0]['share_holders']
        previous = df.iloc[1]['share_holders']

        if previous > 0:
            change_pct = (previous - latest) / previous * 100  # 正数=户数减少，筹码集中
            return change_pct
        else:
            return None
    except Exception as e:
        log.error(f"获取股东户数变化失败 {stock_code}: {str(e)}")
        return None

# ==================== 形态识别模块  ====================
def identify_A_kill(stock_code, end_date):
    """放宽版的A杀识别"""
    try:
        price_data = get_price(stock_code, end_date=end_date, count=200, 
                             fields=['close', 'high', 'low'])
        
        if len(price_data) < 60:
            return {'has_A_kill': True}
        
        close_prices = price_data['close'].values
        high_prices = price_data['high'].values
        
        max_price = np.max(high_prices)
        min_price = np.min(close_prices)
        
        total_change = (max_price - min_price) / min_price * 100
        
        if total_change > 30:
            return {
                'has_A_kill': True,
                'quality_score': 60,
                'A_bottom': float(min_price),
                'A_bottom_date': price_data.index[np.argmin(close_prices)]
            }
        
        return {'has_A_kill': True}
        
    except:
        return {'has_A_kill': True}


def identify_three_waves(stock_code, start_date, end_date):
    """
    识别三波拉升形态
    """
    try:
        # 获取数据
        days_needed = (end_date - start_date).days + 100
        price_data = get_price(
            stock_code,
            end_date=end_date,
            count=days_needed,
            frequency='daily',
            fields=['open', 'close', 'high', 'low', 'volume']
        )
        
        if price_data is None:
            return {'confirmed': False, 'reason': '数据获取失败'}
        
        # 找到起始位置
        price_data.index = pd.to_datetime(price_data.index)
        start_idx = None
        for i, date in enumerate(price_data.index):
            if date.date() >= start_date:
                start_idx = i
                break
        
        if start_idx is None or len(price_data) - start_idx < 30:
            return {'confirmed': False, 'reason': '数据长度不足'}
        
        data = price_data.iloc[start_idx:].copy()
        closes = data['close'].values
        
        # 寻找波段
        waves = []
        support_levels = []
        current_idx = 0
        wave_type = 'up'
        
        while current_idx < len(data) - 10 and len(waves) < 8:
            # 寻找波段起点
            if wave_type == 'up':
                # 寻找低点
                best_low_idx = current_idx
                best_low = closes[current_idx]
                
                for i in range(current_idx, min(current_idx + 10, len(data))):
                    if closes[i] < best_low:
                        best_low = closes[i]
                        best_low_idx = i
                
                start_idx = best_low_idx
                start_price = best_low
            
            else:  # down
                # 寻找高点
                best_high_idx = current_idx
                best_high = closes[current_idx]
                
                for i in range(current_idx, min(current_idx + 10, len(data))):
                    if closes[i] > best_high:
                        best_high = closes[i]
                        best_high_idx = i
                
                start_idx = best_high_idx
                start_price = best_high
            
            # 寻找波段终点
            end_idx = None
            end_price = None
            
            for i in range(start_idx + 5, min(start_idx + 30, len(data))):
                current_price = closes[i]
                
                if wave_type == 'up':
                    current_rise = (current_price - start_price) / start_price * 100
                    if current_rise >= 10:  # 达到最小涨幅
                        # 检查是否是局部高点
                        if i < len(data) - 3:
                            if (current_price >= max(closes[max(0, i-3):i+1]) and 
                                current_price >= max(closes[i+1:min(len(data), i+4)])):
                                end_idx = i
                                end_price = current_price
                                break
                
                else:  # down
                    current_fall = (current_price - start_price) / start_price * 100
                    if current_fall <= -5:  # 回调达到5%
                        # 检查是否是局部低点
                        if i < len(data) - 3:
                            if (current_price <= min(closes[max(0, i-3):i+1]) and 
                                current_price <= min(closes[i+1:min(len(data), i+4)])):
                                end_idx = i
                                end_price = current_price
                                break
            
            if end_idx is None:
                # 没找到合适的终点，使用最大允许天数
                end_idx = min(start_idx + 29, len(data) - 1)
                end_price = closes[end_idx]
            
            # 计算波段特征
            change_pct = (end_price - start_price) / start_price * 100
            
            # 记录支撑位（拉升波中的放量阳线低点）
            support_level = None
            if wave_type == 'up':
                wave_data = data.iloc[start_idx:end_idx+1]
                volumes = wave_data['volume'].values
                avg_volume = np.mean(volumes) if len(volumes) > 0 else 0
                
                for i in range(len(wave_data)):
                    idx = start_idx + i
                    if (data.iloc[i]['close'] > data.iloc[i]['open'] and 
                        volumes[i] > avg_volume * 1.2):
                        current_low = data.iloc[i]['low']
                        if support_level is None or current_low < support_level:
                            support_level = current_low
                
                if support_level:
                    support_levels.append(float(support_level))
            
            # 记录波段
            waves.append({
                'wave_type': wave_type,
                'start_date': data.index[start_idx],
                'end_date': data.index[end_idx],
                'start_price': float(start_price),
                'end_price': float(end_price),
                'change_pct': float(change_pct),
                'duration': end_idx - start_idx + 1,
                'support_level': float(support_level) if support_level else None
            })
            
            # 准备下一个波段
            current_idx = end_idx + 1
            wave_type = 'down' if wave_type == 'up' else 'up'
        
        # 分析结果
        up_waves = [w for w in waves if w['wave_type'] == 'up']
        
        if len(up_waves) < 3:
            return {'confirmed': False, 'reason': f'拉升波不足: {len(up_waves)}'}
        
        # 检查形态质量
        wave_highs = [w['end_price'] for w in up_waves[:3]]
        
        # 检查高点是否抬高
        if not (wave_highs[0] < wave_highs[1] < wave_highs[2]):
            return {'confirmed': False, 'reason': '高点未逐步抬高'}
        
        # 检查支撑位
        if len(support_levels) < 2:
            return {'confirmed': False, 'reason': '支撑位不足'}
        
        # 质量评分
        score = 0
        for wave in up_waves[:3]:
            if 10 <= wave['change_pct'] <= 20:
                score += 10
        
        total_days = sum(w['duration'] for w in waves)
        if total_days <= 90:
            score += 20
        
        if len(support_levels) >= 2:
            score += 20
        
        if score < g.params['min_wave_score']:
            return {'confirmed': False, 'reason': f'质量分不足: {score:.1f}'}
        
        # 计算总涨幅
        total_rise = (wave_highs[-1] - waves[0]['start_price']) / waves[0]['start_price'] * 100
        
        return {
            'confirmed': True,
            'wave_count': len(waves),
            'waves': waves,
            'support_levels': support_levels[:3],
            'wave_highs': wave_highs,
            'wave3_high': float(wave_highs[-1]),
            'strongest_support': float(max(support_levels[:3])) if support_levels else None,
            'total_rise_pct': float(total_rise),
            'total_days': total_days,
            'quality_score': min(100, score),
            'reason': '三波拉升确认'
        }
        
    except Exception as e:
        return {'confirmed': False, 'reason': f'识别失败: {str(e)}'}

# ==================== 洗盘阶段检测 ====================
def check_consolidation(stock_code, wave3_high, support_levels, end_date):
    """
    检查是否处于洗盘阶段
    """
    try:
        # 获取最近60日数据
        price_data = get_price(
            stock_code,
            end_date=end_date,
            count=60,
            frequency='daily',
            fields=['close', 'volume']
        )
        
        if price_data is None or len(price_data) < 20:
            return None
        
        closes = price_data['close'].values
        
        if not support_levels:
            return None
        
        strongest_support = max(support_levels)
        
        # 检查价格区间
        in_range_count = 0
        for price in closes:
            if strongest_support * 0.98 <= price <= wave3_high * 1.02:
                in_range_count += 1
        
        in_range_ratio = in_range_count / len(closes)
        
        # 检查成交量
        volumes = price_data['volume'].values
        if len(volumes) >= 20:
            recent_vol = np.mean(volumes[-10:])
            early_vol = np.mean(volumes[:10])
            volume_ratio = recent_vol / early_vol if early_vol > 0 else 1
        else:
            volume_ratio = 1
        
        current_price = closes[-1]
        price_position = (current_price - strongest_support) / (wave3_high - strongest_support) * 100 if wave3_high > strongest_support else 50
        
        return {
            'is_consolidating': in_range_ratio >= 0.7 and volume_ratio < 1.5,
            'in_range_ratio': in_range_ratio,
            'volume_ratio': volume_ratio,
            'current_price': float(current_price),
            'support_level': float(strongest_support),
            'resistance_level': float(wave3_high),
            'price_position': price_position
        }
        
    except Exception as e:
        return None

# ==================== 交易信号生成 (集成股东户数因子) ====================
def generate_trade_signal(stock_code, stock_name, price_change, turnover_ratio, context):
    """
    生成交易信号 - 使用新的止损止盈参数
    """
    try:
        current_date = context.current_dt.date()
        
        # 1. 检查A杀 
        a_kill = identify_A_kill(stock_code, current_date)
        if not a_kill['has_A_kill']:
            return None
        
        # 2. 检查三波拉升
        three_waves = identify_three_waves(
            stock_code,
            a_kill['A_bottom_date'].date(),
            current_date
        )
        
        if not three_waves['confirmed']:
            return None
        
        # 3. 检查洗盘阶段
        consolidation = check_consolidation(
            stock_code,
            three_waves['wave3_high'],
            three_waves['support_levels'],
            current_date
        )
        
        if not consolidation or not consolidation['is_consolidating']:
            return None
        
        # 4. 获取当天数据
        today_data = get_price(
            stock_code,
            end_date=current_date,
            count=1,
            frequency='daily',
            fields=['open', 'close', 'high', 'low', 'volume']
        )
        
        if today_data is None or today_data.empty:
            return None
        
        today_close = today_data['close'].iloc[0]
        today_high = today_data['high'].iloc[0]
        
        # 5. 获取股东户数变化 (新增)
        shareholder_change = get_shareholder_change(stock_code, current_date)
        
        # 6. 突破与量能判断 (逻辑保持不变)
        resistance = three_waves['wave3_high']
        breakthrough = today_close > resistance
        early_break = (today_high > resistance * 1.01 and 
                      today_close > today_data['open'].iloc[0] * 1.02)
        
        avg_volume_5 = get_price(
            stock_code,
            end_date=current_date,
            count=5,
            frequency='daily',
            fields=['volume']
        )['volume'].mean() if len(get_price(stock_code, end_date=current_date, count=5, frequency='daily')) >= 5 else today_data['volume'].iloc[0]
        
        volume_ratio = today_data['volume'].iloc[0] / avg_volume_5 if avg_volume_5 > 0 else 1
        
        # 7. 信号强度判断
        signal_strength = 'weak'
        position_ratio = 0
        
        if breakthrough and early_break and volume_ratio > 1.5:
            signal_strength = 'strong'
            position_ratio = g.params['position_ratio']
        elif breakthrough and volume_ratio > 1.2:
            signal_strength = 'medium'
            position_ratio = g.params['position_ratio'] * 0.6
        elif today_high > resistance and volume_ratio > 1.0:
            signal_strength = 'weak'
            position_ratio = g.params['position_ratio'] * 0.3
        
        if position_ratio <= 0:
            return None
        
    # 在构建信号时，使用新的参数计算止损止盈价
        signal = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'signal_strength': signal_strength,
            'position_ratio': position_ratio,
            'entry_price': float(today_close),
            # 使用新参数计算：
            'stop_loss': float(consolidation['support_level'] * (1 - g.params['immediate_stop_loss'])),
            'take_profit': float(today_close * (1 + g.params['immediate_take_profit'])),
            'shareholder_change': shareholder_change,                   # 新增关键字段
            'volume_ratio': volume_ratio,
            'price_change': price_change,
            'turnover_ratio': turnover_ratio,
            'buy_time': context.current_dt
        }
        return signal
    except Exception as e:
        log.error(f"生成交易信号失败 {stock_code}: {str(e)}")
        return None    

# ==================== 持仓管理 (按新规则拆分和修改) ====================
def check_immediate_stops(context):
    """
    立即止损止盈检查 - 使用优化后的参数
    每分钟由 handle_data 调用
    """
    if not g.positions:
        return
    
    current_time = context.current_dt.time()
    
    # 避开特殊时段
    if current_time.hour == 9 and current_time.minute < 30:
        return
    if current_time.hour == 14 and current_time.minute >= 55:
        return
    
    for stock_code, position in list(g.positions.items()):
        try:
            # 跳过已标记卖出的持仓
            if position.get('selling', False):
                continue
            
            # 检查实际持仓
            if stock_code not in context.portfolio.positions:
                continue
            
            current_amount = context.portfolio.positions[stock_code].total_amount
            if current_amount <= 0:
                continue
            
            # 获取当前价格
            current_data = get_current_data()[stock_code]
            current_price = current_data.last_price
            
            buy_price = position['buy_price']
            profit_pct = (current_price - buy_price) / buy_price * 100
            
            # =========== 使用新参数的条件判断 ===========
            # 1. 立即止损：亏损达到-5%
            if profit_pct <= -g.params['immediate_stop_loss'] * 100:
                log.info(f"🚨 立即止损触发: {stock_code}")
                log.info(f"   买入价: {buy_price:.2f}, 当前价: {current_price:.2f}, 亏损: {profit_pct:.1f}%")
                
                # 市价卖出
                order_target(stock_code, 0)
                
                # 更新状态
                g.positions[stock_code]['selling'] = True
                g.positions[stock_code]['sell_reason'] = f"立即止损({profit_pct:.1f}%)"
                g.positions[stock_code]['sell_time'] = context.current_dt
                
                # 记录资金释放
                log.info(f"   资金已释放，当前可用: {context.portfolio.available_cash:.2f}")
                continue
            
            # 2. 立即止盈：盈利达到12%
            if profit_pct >= g.params['immediate_take_profit'] * 100:
                log.info(f"🎯 立即止盈触发: {stock_code}")
                log.info(f"   买入价: {buy_price:.2f}, 当前价: {current_price:.2f}, 盈利: {profit_pct:.1f}%")
                
                # 市价卖出
                order_target(stock_code, 0)
                
                # 更新状态
                g.positions[stock_code]['selling'] = True
                g.positions[stock_code]['sell_reason'] = f"立即止盈({profit_pct:.1f}%)"
                g.positions[stock_code]['sell_time'] = context.current_dt
                
                # 记录资金释放
                log.info(f"   资金已释放，当前可用: {context.portfolio.available_cash:.2f}")
                # ======================================
                
        except Exception as e:
            log.error(f"立即止损止盈检查错误 {stock_code}: {str(e)}")
            
def check_tail_position(context):
    """
    14:55执行，检查尾盘止盈(6%)和时间止损(4天)
    使用优化后的参数
    """
    if not g.positions:
        log.info("14:55 - 无持仓需要检查")
        return
    
    current_date = context.current_dt.date()
    log.info(f"=== 14:55尾盘持仓检查（新参数） ===")
    log.info(f"当前可用资金: {context.portfolio.available_cash:.2f}")
    
    for stock_code, position in list(g.positions.items()):
        try:
            # 跳过已标记卖出的持仓
            if position.get('selling', False):
                continue
            
            # 检查实际持仓
            if stock_code not in context.portfolio.positions:
                continue
            
            current_amount = context.portfolio.positions[stock_code].total_amount
            if current_amount <= 0:
                continue
            
            # 获取当前价格
            current_data = get_current_data()[stock_code]
            current_price = current_data.last_price
            
            buy_price = position['buy_price']
            profit_pct = (current_price - buy_price) / buy_price * 100
            hold_days = (current_date - position['buy_time'].date()).days
            
            # =========== 使用新参数的条件判断 ===========
            should_sell = False
            reason = ""
            
            # 1. 尾盘止盈：盈利达到6%
            if profit_pct >= g.params['tail_take_profit'] * 100:
                should_sell = True
                reason = f"尾盘止盈({profit_pct:.1f}%)"
            
            # 2. 时间止损：持有4天
            elif hold_days >= g.params['time_stop_days']:
                should_sell = True
                if profit_pct > 0:
                    reason = f"时间止盈({hold_days}天, 盈利{profit_pct:.1f}%)"
                else:
                    reason = f"时间止损({hold_days}天, 亏损{profit_pct:.1f}%)"
            # ======================================
            
            if should_sell:
                log.info(f"尾盘卖出: {stock_code} - {reason}")
                log.info(f"  买入价: {buy_price:.2f}, 当前价: {current_price:.2f}, 盈亏: {profit_pct:.1f}%")
                
                # 市价卖出
                order_target(stock_code, 0)
                
                # 标记状态
                g.positions[stock_code]['selling'] = True
                g.positions[stock_code]['sell_reason'] = reason
                g.positions[stock_code]['sell_time'] = context.current_dt
                
        except Exception as e:
            log.error(f"尾盘检查错误 {stock_code}: {str(e)}")
    
    # 清理已卖出的持仓记录
    cleanup_sold_positions(context)
    log.info(f"尾盘检查后可用资金: {context.portfolio.available_cash:.2f}")

def cleanup_sold_positions(context):
    """清理已卖出的持仓记录"""
    stocks_to_remove = []
    for stock_code, position in list(g.positions.items()):
        if position.get('selling', False):
            current_amount = 0
            if stock_code in context.portfolio.positions:
                current_amount = context.portfolio.positions[stock_code].total_amount
            if current_amount <= 0:
                stocks_to_remove.append(stock_code)
    
    for stock_code in stocks_to_remove:
        if stock_code in g.positions:
            del g.positions[stock_code]

# ==================== 交易逻辑  ====================
def trade_logic(context):
    """
    主交易逻辑 - 14:56执行
    按综合评分(信号强度 + 股东户数下降)排序买入
    """
    if not g.trading_enabled:
        return
    
    current_time = context.current_dt.time()
    if not (current_time.hour == 14 and current_time.minute == 56):
        return
    
    log.info(f"交易日期: {context.current_dt.date()} 14:56")
    log.info(f"当前可用资金: {context.portfolio.available_cash:.2f}")
    
    current_positions = len([p for p in context.portfolio.positions.values() if p.total_amount > 0])
    if current_positions >= g.params['max_positions']:
        log.info(f"已达最大持仓{current_positions}只，不再买入")
        return
    
    available_cash = context.portfolio.available_cash
    if available_cash < 10000:
        log.info(f"可用资金不足: {available_cash:.2f}")
        return
    
    position_value = min(available_cash * g.params['position_ratio'], 
                        available_cash / (g.params['max_positions'] - current_positions))
    if position_value < 10000:
        log.info(f"单只买入金额不足: {position_value:.2f}")
        return
    
    current_date = context.current_dt.date()
    prev_date = get_trade_days(end_date=current_date, count=2)[0] if len(get_trade_days(end_date=current_date, count=2)) >= 2 else current_date
    
    # 基础筛选 (涨幅>5%，换手5%-20%)
    basic_candidates = []
    for stock_code, stock_name in g.stock_list:
        try:
            price_data = get_price(stock_code, end_date=current_date, count=2,
                                 fields=['close', 'high', 'volume', 'low'])
            if price_data.empty or len(price_data) < 2:
                continue
                
            pre_close = price_data['close'].iloc[0]
            today_high = price_data['high'].iloc[1]
            today_volume = price_data['volume'].iloc[1]
            
            price_change = (today_high - pre_close) / pre_close * 100
            
            circ_df = get_fundamentals(
                query(valuation.circulating_cap).filter(valuation.code == stock_code),
                date=prev_date
            )
            if circ_df.empty:
                continue
            
            circulating_shares = circ_df['circulating_cap'].iloc[0] * 10000
            turnover_ratio = (today_volume / circulating_shares) * 100
            
            if (g.params['min_turnover'] < turnover_ratio <= g.params['max_turnover'] and 
                price_change > g.params['min_price_change'] and 
                not is_close_limit_up(stock_code, current_date)):
                basic_candidates.append({
                    'code': stock_code,
                    'name': stock_name,
                    'price_change': price_change,
                    'turnover_ratio': turnover_ratio
                })
                    
        except Exception as e:
            continue
    
    log.info(f"基础筛选通过: {len(basic_candidates)}只")
    
    # 生成交易信号 (集成形态和股东户数)
    trade_signals = []
    for candidate in basic_candidates:
        signal = generate_trade_signal(
            candidate['code'],
            candidate['name'],
            candidate['price_change'],
            candidate['turnover_ratio'],
            context
        )
        if signal:
            trade_signals.append(signal)
    
    if not trade_signals:
        log.info("无有效交易信号")
        return
    
    # ===== 核心修改：按综合评分排序 =====
    def calculate_composite_score(signal):
        """综合评分 = 信号强度分 + 调整后的股东户数变化加分"""
        strength_score = {'strong': 100, 'medium': 60, 'weak': 30}.get(signal['signal_strength'], 0)
        
        shareholder_bonus = 0
        change = signal.get('shareholder_change')
        # 修改逻辑：股东户数减少（筹码集中）为正加分，增加（筹码分散）为负减分
        if change is not None:
            # 每减少1%加10分，每增加1%减10分，设置上下限
            shareholder_bonus = change * (-10)  # 注意：change为正表示户数增加，所以用负号
            shareholder_bonus = max(-50, min(shareholder_bonus, 50))  # 限制在-50到50分之间
        
        return strength_score + shareholder_bonus
    
    # 按综合评分从高到低排序
    trade_signals.sort(key=lambda x: calculate_composite_score(x), reverse=True)
    
    # 买入评分最高的股票
    buy_count = 0
    max_buy = min(2, g.params['max_positions'] - current_positions)
    
    for signal in trade_signals[:max_buy * 2]:
        if buy_count >= max_buy:
            break
        
        stock_code = signal['stock_code']
        if stock_code in g.positions or (stock_code in context.portfolio.positions and context.portfolio.positions[stock_code].total_amount > 0):
            continue
        
        try:
            current_price = get_current_data()[stock_code].last_price
            log.info(f"买入 [{buy_count+1}]: {signal['stock_name']} ({stock_code})")
            log.info(f"  综合评分: {calculate_composite_score(signal):.1f} | 涨幅: {signal['price_change']:.1f}% | 换手: {signal['turnover_ratio']:.1f}%")
            if signal.get('shareholder_change') is not None:
                log.info(f"  股东户数变化: {signal['shareholder_change']:+.1f}%")
            
            order_result = order_value(stock_code, position_value)
            
            if order_result:
                g.positions[stock_code] = {
                    'buy_price': current_price,
                    'buy_time': context.current_dt,
                    'quantity': position_value / current_price if current_price > 0 else 0,
                    'selling': False,
                    'stop_loss': current_price * 0.97,   # 3%止损
                    'take_profit': current_price * 1.08  # 8%止盈
                }
                buy_count += 1
                log.info(f"  买入成功，金额: {position_value:.0f}")
            else:
                log.warning(f"  买入失败")
                
        except Exception as e:
            log.error(f"买入错误 {stock_code}: {str(e)}")
    
    if buy_count > 0:
        log.info(f"成功买入 {buy_count} 只股票")
    else:
        log.info("未能买入任何股票")
    
    log.info(f"操作后可用资金: {context.portfolio.available_cash:.2f}")

# ==================== 辅助函数 ====================
def morning_cleanup(context):
    """开盘清理"""
    orders = get_open_orders()
    canceled_count = 0
    for order_list in orders.values():
        for o in order_list:
            if o.status in ['open', 'pending']:
                cancel_order(o)
                canceled_count += 1
    
    if canceled_count > 0:
        log.info(f"取消 {canceled_count} 个未成交订单")
    
    stocks_to_remove = []
    for stock_code, position in list(g.positions.items()):
        if position.get('selling', False):
            stocks_to_remove.append(stock_code)
    
    for stock_code in stocks_to_remove:
        if stock_code in g.positions:
            del g.positions[stock_code]
    
    log.info(f"交易日: {context.current_dt.date()}")
    log.info(f"开盘资金 - 可用: {context.portfolio.available_cash:.2f}, 总资产: {context.portfolio.total_value:.2f}")
    log.info(f"当前持仓数: {len(g.positions)}")

def is_close_limit_up(stock_code, current_date):
    """判断是否收盘涨停"""
    try:
        price_data = get_price(stock_code, end_date=current_date, count=2, fields=['close'])
        if len(price_data) < 2:
            return False
        pre_close = price_data['close'].iloc[0]
        today_close = price_data['close'].iloc[1]
        change_pct = (today_close - pre_close) / pre_close * 100
        if stock_code.startswith('68') or stock_code.startswith('30'):
            return change_pct >= 19.9
        elif stock_code.startswith('00') or stock_code.startswith('60'):
            return change_pct >= 9.9
        return False
    except:
        return False

def handle_data(context, data):
    """
    每分钟自动运行，用于检查立即止损止盈
    这是聚宽框架的固定函数，必须用这个名称
    """
    # 如果交易被禁用，直接返回
    if not g.trading_enabled:
        return
    
    current_time = context.current_dt.time()
    
    # 可以添加时间过滤，避免在某些时段检查
    # 例如：避免在开盘集合竞价和尾盘检查（尾盘有专门函数）
    if current_time.hour == 9 and current_time.minute < 30:  # 开盘集合竞价
        return
    if current_time.hour == 14 and current_time.minute >= 55:  # 尾盘交给专门函数
        return
    
    # 调用立即止损止盈检查
    check_immediate_stops(context)
    
    # 可选：每分钟清理已卖出的持仓记录
    cleanup_sold_positions(context)
def after_trading_end(context):
    """盘后总结"""
    log.info(f"交易日结束总结:")
    log.info(f"持仓数量: {len(g.positions)}")
    for stock_code, position in g.positions.items():
        try:
            current_price = get_current_data()[stock_code].last_price
            profit_pct = (current_price - position['buy_price']) / position['buy_price'] * 100
            log.info(f"  {stock_code}: 成本 {position['buy_price']:.2f} | 现价 {current_price:.2f} | 盈亏 {profit_pct:+.1f}%")
        except:
            continue
