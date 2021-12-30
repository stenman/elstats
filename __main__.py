#!/usr/bin/env python3

from python_graphql_client import GraphqlClient
import asyncio
import elstatsdao

# def main():
#     # Only used to initiate DB!
#     elstatsdao.create_tables()

def main():
    client = GraphqlClient(endpoint="wss://api.tibber.com/v1-beta/gql/subscriptions")
    authorization = "authorizationtoken"

    query = """
    subscription {
        liveMeasurement(homeId:"homeid") {
            timestamp
            power
            lastMeterConsumption	
            accumulatedConsumption
            accumulatedConsumptionLastHour
            accumulatedCost
            currency
            minPower
            averagePower
            maxPower
            powerReactive
            powerFactor
            voltagePhase1
            voltagePhase2
            voltagePhase3
            currentL1
            currentL2
            currentL3
            signalStrength
        }
    }
    """
    asyncio.run(client.subscribe(query=query, headers={'Authorization': authorization}, handle=save_stats))


def save_stats(data):
    elstatsdao.save_pulse_stats(data)


if __name__ == '__main__':
    main()
