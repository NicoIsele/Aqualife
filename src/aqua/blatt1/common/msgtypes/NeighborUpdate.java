package aqua.blatt1.common.msgtypes;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class NeighborUpdate implements Serializable {

    private InetSocketAddress leftNeighbor;
    private InetSocketAddress rightNeighbor;

    public NeighborUpdate(InetSocketAddress leftNeighbor, InetSocketAddress rightNeighbor) {
        this.leftNeighbor = leftNeighbor;
        this.rightNeighbor = rightNeighbor;
    }
    public InetSocketAddress getLeftNeighbor() {
        return leftNeighbor;
    }
    public InetSocketAddress getRightNeighbor() {
        return  rightNeighbor;
    }
}