# Market Maker Graduation Checklist
## Paper Trading Requirements (must ALL pass)

- [ ] Running for minimum 7 days
- [ ] Minimum 50 round trips completed
- [ ] Average spread collected > $0.05 per round trip
- [ ] No circuit breaker triggered
- [ ] Fee monitor shows $0.00 fees
- [ ] Daily income consistent (not just lucky days)
- [ ] Max daily loss never exceeded 3% of capital

## To graduate to live trading:
1. All boxes above checked
2. Manual review of logs
3. Update MM_PAPER_TRADING = False in config.py
4. Restart service
5. Monitor first 24 hours closely
