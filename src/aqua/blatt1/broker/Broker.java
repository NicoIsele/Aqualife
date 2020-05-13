package aqua.blatt1.broker;

import aqua.blatt1.common.Direction;
import aqua.blatt1.common.FishModel;
import aqua.blatt1.common.msgtypes.*;
import messaging.Endpoint;
import messaging.Message;

import javax.swing.*;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Broker {

    private class BrokerTask {

        public void brokerTask(Message msg) {
            if (msg.getPayload() instanceof RegisterRequest) {
                synchronized (client) {register(msg);}
            }

            if (msg.getPayload() instanceof DeregisterRequest) {
                synchronized (client) {deregister(msg);}
            }
            if (msg.getPayload() instanceof HandoffRequest) {
                lock.writeLock().lock();
                HandoffRequest handoffRequest = (HandoffRequest) msg.getPayload();
                InetSocketAddress inetSocketAddress = msg.getSender();
                handOffFish(handoffRequest, inetSocketAddress);
                lock.writeLock().unlock();
            }
            if (msg.getPayload() instanceof PoisonPill) {
                System.exit(0);
            }
        }
    }

    public static void main(String[] args) {
        Broker broker = new Broker();
        broker.broker();
    }

    Endpoint endpoint = new Endpoint(4711);
    ClientCollection client = new ClientCollection();
    ExecutorService executor = Executors.newFixedThreadPool(5);
    int counter = 0;
    ReadWriteLock lock = new ReentrantReadWriteLock();
    volatile boolean stopRequest = false;

    public void broker(){

        executor.execute(new Runnable() {
            @Override
            public void run() {
                JOptionPane.showMessageDialog(null, "OK drücken, um Server zu stoppen");
                    stopRequest = true;

            }
        });

        while( !stopRequest ) {
            Message msg = endpoint.blockingReceive();
            BrokerTask brokerTask = new BrokerTask();
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    brokerTask.brokerTask(msg);
                }
            });

        }
    }
    private void register(Message msg) {
        InetSocketAddress sender = msg.getSender();
        String id = "tank"+ counter;
        counter++;
        client.add( id, sender);


        /********** Aufgabe3 **************/

        int index = client.indexOf(sender);
        InetSocketAddress leftNeighborOf = (InetSocketAddress) client.getLeftNeighorOf(index);
        InetSocketAddress rightNeighborOf = (InetSocketAddress) client.getRightNeighorOf(index);

        InetSocketAddress initialLeftNeighbor = (InetSocketAddress) client.getLeftNeighorOf(client.indexOf(leftNeighborOf));
        InetSocketAddress initialRightNeighbor = (InetSocketAddress) client.getRightNeighorOf(client.indexOf(rightNeighborOf));

        if (client.size() == 1){
            endpoint.send(sender, new NeighborUpdate(leftNeighborOf, rightNeighborOf));
            endpoint.send(sender, new Token());
        } else {
            endpoint.send(sender, new NeighborUpdate(leftNeighborOf, rightNeighborOf));
            endpoint.send(leftNeighborOf, new NeighborUpdate(initialLeftNeighbor, sender));
            endpoint.send(rightNeighborOf, new NeighborUpdate(sender, initialRightNeighbor));
        }
        /************************/

        endpoint.send(msg.getSender(), new RegisterResponse(id));
    }

    private void deregister(Message msg) {
        /********** Aufgabe3 **************/
        InetSocketAddress sender = msg.getSender();
        int index = client.indexOf(sender);
        InetSocketAddress leftNeighborOf = (InetSocketAddress) client.getLeftNeighorOf(index);
        InetSocketAddress rightNeighborOf = (InetSocketAddress) client.getRightNeighorOf(index);

        InetSocketAddress initialLeftNeighbor = (InetSocketAddress) client.getLeftNeighorOf(client.indexOf(leftNeighborOf));
        InetSocketAddress initialRightNeighbor = (InetSocketAddress) client.getRightNeighorOf(client.indexOf(rightNeighborOf));

        endpoint.send(leftNeighborOf, new NeighborUpdate(initialLeftNeighbor, rightNeighborOf));
        endpoint.send(rightNeighborOf, new NeighborUpdate(leftNeighborOf, initialRightNeighbor));
        /************************/

        client.remove(index);

    }

    private void handOffFish(HandoffRequest handoffRequest, InetSocketAddress inetSocketAddress) {
        int index = client.indexOf(inetSocketAddress);
        FishModel fishModel = handoffRequest.getFish();
        Direction direction = fishModel.getDirection();

        InetSocketAddress neighborReceiver;
        if (direction == Direction.LEFT) {
            neighborReceiver = (InetSocketAddress) client.getLeftNeighorOf(index);
        }
        else {
            neighborReceiver = (InetSocketAddress) client.getRightNeighorOf(index);
        }

        endpoint.send(neighborReceiver, handoffRequest);
    }
}
