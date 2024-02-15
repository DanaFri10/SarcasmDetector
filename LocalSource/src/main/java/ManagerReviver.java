public class ManagerReviver extends Thread{
    public static boolean STOP = false;
    @Override
    public void run() {
        while(!STOP) {
            Main.startManager();
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
