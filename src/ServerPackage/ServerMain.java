package ServerPackage;


import ClassCollection.CollectionTask;
import com.sun.security.ntlm.Server;
import org.springframework.security.crypto.scrypt.SCryptPasswordEncoder;
import packet.BDconnector;
import packet.CommandA;
import packet.Person;
import ServerPackage.IWillNameItLater.WrongTypeOfFieldException;
import ServerPackage.IWillNameItLater.receiver;


import javax.xml.crypto.Data;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Exchanger;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * Основной класс, реализующий сервер
 * @author Maxim Antonov and Andrey Lyubkin
 */
public class ServerMain
{

    private static boolean AUTHORIZATIONCHECK=false;
    private static boolean BDCHECKING=false;
    private static ReentrantLock lock;  // блокировка
   private static Condition cond;  // условие блокировки

    private static Map<String, SocketAddress> addressMap = new HashMap<>();

    private static String ACCESS;

    private static Log4J2 SustemOut = new Log4J2(System.out);
    private static CollectionTask collectionTask;
    private static receiver CU;
    private static String str;
    private static boolean isRight=false;
    private static Map<String, Long> ConnectionKeies= new HashMap<>();
    private static ThreadLocal<SocketAddress> socketAddress=new ThreadLocal<>();
    //private static ThreadLocal<ByteBuffer> buf = new ThreadLocal<>();
    private static ByteBuffer buf;
    private static ByteBuffer checkBuffer;

    private static int cpuCost = (int) Math.pow(2, 14); // factor to increase CPU costs
    private static int memoryCost = 8;      // increases memory usage
    private static int parallelization = 1; // currently not supported by Spring Security
    private static int keyLength = 32;      // key length in bytes
    private static int saltLength = 64;     // salt length in bytes
    private static BDconnector bc;
    private static Object sync;
    private static Object valera;
    private static Transmitter transmitter;
    private static SCryptPasswordEncoder sCryptPasswordEncoder = new SCryptPasswordEncoder(
            cpuCost,
            memoryCost,
            parallelization,
            keyLength,
            saltLength);

    /**
     * Метод, выполняющий запуск сервера
     * @param args
     */
    public static void main(String args[]) throws Exception
    {


        sync = new Object();
        valera = new Object();
        bc =new BDconnector(3748);

        try {

            DatagramChannel chan = DatagramChannel.open();
            chan.socket().bind(new InetSocketAddress(1229));
            chan.configureBlocking(false);
            Selector selector = Selector.open();
            chan.register(selector, SelectionKey.OP_READ);
            checkBuffer = ByteBuffer.allocate(4 * 1024);


            collectionTask = new CollectionTask();
            try {
                collectionTask.load(bc);
                CU = new CollectionUnit(collectionTask, bc);
            }catch (NullPointerException ex) {
                System.out.println("Как вы это сделали?");
            }
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);
            ThreadPoolExecutor sender = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);
            RequestHandler rh = new RequestHandler("Приниматор", chan);
            transmitter = new Transmitter("Отправлятор", chan, rh);
            //buf = ByteBuffer.allocate(4*1024);
            //buf.put(0, (byte) -84);

            while (true) {
/*                if (selector.select() > 0) {
                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> iter = selectedKeys.iterator();

                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();

                        System.out.println(key);
                        if (key.isReadable()) {
                            DatagramChannel channel = (DatagramChannel) key.channel();
                            //executor.awaitTermination(1, TimeUnit.HOURS);*/
                buf = ByteBuffer.allocate(4 * 1024);
                synchronized (sync) {
                    executor.execute(rh);
                    sync.wait(1000);

                    SocketAddress check = rh.getFrom();//suka ka je ti xaebal
                    if (check != null) {
                        //System.out.println("loh");
                        //buf = rh.getByteBuffer();
                        //System.out.println(buf.get(0));

                        //System.out.println(rh.getByteBuffer());
                        //System.out.println(Thread.currentThread().getName());
                        //valera.wait(1000);
                        ServerThread serverThread = new ServerThread(chan, sender, transmitter, check, buf);
                        serverThread.start();
                        //serverThread.join();
                    }
                }
            }
                        /*executor.shutdown();
                            str = SustemOut.sendTxt() + "\n$";
                            printsmth(channel, from);*/
                        /*}
                        iter.remove();
                    }
                }*/
                //executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
        }catch (IllegalAccessError e){
            SustemOut.println("Вийди звiдси розбiйник");
        }


    }


    //dlya zapisi v BD

    /**
     * Запись нового пользователя в БД
     * @param con Соединение с БД
     * @param Login Логин пользователя
     * @param Pass Пароль пользователя
     * @throws SQLException
     */
    private synchronized static void insert(Connection con, String Login, String Pass) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("INSERT INTO JC_CONTACT (LOGIN,pass) VALUES (?, ?)");
        stmt.setString(1, Login);
        stmt.setString(2, Pass);
        stmt.executeUpdate();
        stmt.close();
    }





    //Dlya proverki logina po pd

    /**
     * Авторизация пользователя
     * @param con Соединение с БД
     * @param channel Канал для отправки ответа клиенту
     * @param from Адрес клиента
     * @param login Логин клиента
     * @param pass Пароль клиента
     */
    private synchronized static void CheckLogin(Connection con, DatagramChannel channel, SocketAddress from, String login, String pass) throws SQLException, IOException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM JC_CONTACT");
        while (rs.next()) {
            if ((rs.getString(2).equals(login))&&(sCryptPasswordEncoder.matches(pass,rs.getString(3)))){
                ACCESS=UUID.randomUUID().toString();
                ConnectionKeies.put(ACCESS, System.currentTimeMillis());
                SustemOut.addText("Доступ открыт&"+ACCESS);
                //str = SustemOut.sendTxt()+"\n$";
                //printsmth(channel,from);
                BDCHECKING=true;
                if (!addressMap.containsKey(login)) {
                    addressMap.put(login, from);
                }
                sendToAll("На сервере появился " + login, channel, login);
            }
        }
        if (!BDCHECKING){
            SustemOut.addText("Неверный логин или пароль");
            str = SustemOut.sendTxt()+"\n$";
            //printsmth(channel,from);

        }
        BDCHECKING=false;
        rs.close();
        stmt.close();

    }



    //hui ego znaet tip perepisivaem 4to poluchii v drugoi buffer
    //da eto kostyl shob ne bilo lishnih nullov

    /**
     * Метод, выполняющий преобразование исходного буфера в пригодный для дальнейшей работы
     * @param buffer Исходный буфер
     * @param finalBuffer Буфер, куда будет записано все необходимое из исходного
     * @return Измененный финальный буфер
     */
    private static ByteBuffer getFinalBuffer(ByteBuffer buffer, ByteBuffer finalBuffer){
        for (int i = 0; i < buffer.position(); ++i){
            finalBuffer.put(i, buffer.get(i));
        }
        return finalBuffer;
    }



    //poluchenie logina

    /**
     * Получение логина пользователя из запроса
     * @param buffer Исходный буфер
     * @param finalBuffer Буфер для дальнейшей работы
     * @param from Адрес пользователя
     * @return Логин
     */
    private static String getLogin(ByteBuffer buffer,ByteBuffer finalBuffer, SocketAddress from){

        ByteBuffer finalBuffer_=getFinalBuffer(buffer,finalBuffer);
        if (from != null) {
            buffer.flip();
            CommandA cam = deserialize(finalBuffer_.array());
            String val = cam.getLogin();
            return val;
        }
        return null;
    }


    //poluchenie logina

    /**
     * Получение пароля пользователя из запроса
     * @param buffer Исходный буфер
     * @param finalBuffer Буфер для дальнейшей работы
     * @param from Адрес пользователя
     * @return Пароль
     */
    private static String getPass(ByteBuffer buffer,ByteBuffer finalBuffer, SocketAddress from){

        ByteBuffer finalBuffer_=getFinalBuffer(buffer,finalBuffer);
        if (from != null) {
            buffer.flip();
            CommandA cam = deserialize(finalBuffer_.array());
            String val = cam.getPass();
            return val;
        }
        return null;
    }

    //HZ KAK TUT IZBEJAT DUBLIROVANIya potom eshe podumau
    /**
     * Получение токена пользователя из запроса
     * @param buffer Исходный буфер
     * @param finalBuffer Буфер для дальнейшей работы
     * @param from Адрес пользователя
     * @return Токен
     */
    private static String getAccess(ByteBuffer buffer,ByteBuffer finalBuffer, SocketAddress from){

        ByteBuffer finalBuffer_=getFinalBuffer(buffer,finalBuffer);
        if (from != null) {
            buffer.flip();
            CommandA cam = deserialize(finalBuffer_.array());
            String val = cam.GETACCESS();
            return val;
        }
        return null;
    }

    //polucheniya imya comandi
    /**
     * Получение названия команды пользователя из запроса
     * @param buffer Исходный буфер
     * @param finalBuffer Буфер для дальнейшей работы
     * @param from Адрес пользователя
     * @return Название команды
     */
    public static String getNameCom(ByteBuffer buffer,ByteBuffer finalBuffer, SocketAddress from){
        ByteBuffer finalBuffer_=getFinalBuffer(buffer,finalBuffer);
        if (from != null) {
            buffer.flip();
            CommandA cam = deserialize(finalBuffer_.array());
            String val = cam.toString();
            return val;
        }
        return null;
    }


    //dlya raboti s kollekciei

    /**
     * Основной метод, реализующий выполнение команды пользователя
     * @param buffer Исходный буфер
     * @param finalBuffer Буфер для дальнейшей работы
     * @param from Адрес пользователя
     * @param channel Канал откуда пришел запрос
     * @throws IOException
     * @throws InterruptedException
     */
    private synchronized static void action(ByteBuffer buffer,ByteBuffer finalBuffer, DatagramChannel channel,SocketAddress from) throws IOException, InterruptedException {


        ByteBuffer finalBuffer_=getFinalBuffer(buffer,finalBuffer);
        if (from != null) {
            buffer.flip();
            CommandA cam = deserialize(finalBuffer_.array());
            String val = cam.toString()+cam.getStringToSend();
            CU.setLogin(cam.getLogin());
            if (isRight){
                val=cam.gettoUpdate_b()+cam.getStringToSend();
                isRight=false;


            }else if ((val.equals("update")) && (!isRight)){
                val=cam.gettoUpdate_s();
                isRight=true;
            }
            if (val.equals("remove_by_id")){
                val=cam.getToremove();
            }
            if (val.equals("remove_any_by_nationality")){
                val=cam.getToremovenat();
            }
            if (val.equals("count_less_than_location")){
                val=cam.getLocat();
            }
            if (val.equals("filter_starts_with_name")){
                val=cam.getStartWithName();
            }
            if (val.equals("execute_script")){
                val=cam.getFileName();
            }

            SustemOut.print("----"+from.toString()+"----");
            SustemOut.print("----"+channel.toString()+"----");
            SustemOut.print("----Сервер получил сообщение со стороны клиента: "+ val+"-----");
            //  SustemOut.("Сервер получил сообщение со стороны клиента: "+val);

            try {
                String[] userCommand = val.split("=");
                HashMap<String, String> fields = new HashMap<>();
                if((userCommand[0].equals("update")) && (userCommand.length == 2)){
                    Stream<Person> personStream = CU.getCT().GetCollection().stream();
                    if(personStream.anyMatch(person -> person.getId() == Long.parseLong(userCommand[1]))){
                        SustemOut.addText("Объект с таким id найден"+"\n");
                        isRight=true;
                    }else {SustemOut.addText("Объект с таким id не найден"+"\n");
                    isRight=false;}
                }else {
                    for (int i = 1; i < userCommand.length; i += 2) {
                        fields.put(userCommand[i], userCommand[i + 1]);
                        collectionTask.getCommandMap().get(userCommand[0]).getTransporter().SetParams(fields); //костыль чтоб работал, потом переделать нормально (добавить всем тарнспортер)
                    }
                    collectionTask.getCommandMap().get(userCommand[0]).execute(CU);
                }
            } catch (WrongTypeOfFieldException e) {
                e.printStackTrace();

            }
            if(SustemOut.getLast().equals("Объект с таким id найден"+"\n")) str = SustemOut.sendTxt();
            else str = SustemOut.sendTxt()+"\n$";
            //printsmth(channel,from);

        }
        SustemOut.print( "----Отключение----" );

    }


    /*visrat 4to-to clientu
    private static void printsmth(DatagramChannel channel, SocketAddress from) throws IOException {
        SustemOut.clear();
        ByteBuffer lol = ByteBuffer.wrap(str.getBytes());
        channel.send(lol, from);
        CU.setResponse("");
    }*/

    /**
     * Отправка информации всем авторизованным пользователям кроме отправителя запроса
     * @param s Строка, которую нужно отправить
     * @param channel Канал для отправки
     * @param login Логин, на который не нужно отправлять (автор запроса)
     */
    private static void sendToAll(String s, DatagramChannel channel, String login){
        ByteBuffer kek = ByteBuffer.wrap((s+"\n$").getBytes());
        addressMap.forEach((k,v) -> {
            try {
                if(!(k.equals(login))) {
                    channel.send(kek, v);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    //tut o4evidno

    /**
     * Десериализация запроса
     * @param data Исходный массив
     * @return Команда пользователя
     */
    private static CommandA deserialize(byte[] data){

        try {
            ObjectInputStream iStream = new ObjectInputStream(new ByteArrayInputStream(data));
            CommandA obj = (CommandA) iStream.readObject();
            iStream.close();
            return obj;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Класс, реализующий поток чтения запросов
     */
    static class RequestHandler extends Thread
    {
        private String name;
        private DatagramChannel channel;
        private ByteBuffer byteBuffer;
        private SocketAddress from;
        public RequestHandler(String name, DatagramChannel channel_)
        {
            this.channel=channel_;
            this.name = name;
        }

        public Log4J2 getSustemOut() {
            return SustemOut;
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        @Override
        public void run()
        {
            try {
                synchronized (checkBuffer){
                    synchronized (sync) {
                        //sleep(1000);
                        buf.clear();
                        buf.put(new byte[4 * 1024]);
                        buf.clear();
                        from = channel.receive(buf);
                        if (from != null) {
                            sync.notify();
                            SustemOut.print("----Соединение с клиентом----");
                            SustemOut.print("----Было успешно установлено----");
                        }
                    }
                }

            }
            catch (IOException/* | InterruptedException*/ e)
            {
                e.printStackTrace();
            }
        }


        public SocketAddress getFrom(){ return from;}

        public void setFrom(SocketAddress from) {
            this.from = from;
        }
    }

    /**
     * Класс, реализующий поток отправки ответов пользователям
     */
    static class Transmitter implements Runnable {
        private String name;
        private DatagramChannel channel;
        private RequestHandler rh;
        private SocketAddress from;

        public Transmitter(String name, DatagramChannel chan, RequestHandler rh)
        {
            this.name = name;
            this.channel = chan;
            this.rh=rh;
        }

        public String getName() {
            return name;
        }

        @Override
        public void run()
        {
            try
            {
                        str = SustemOut.sendTxt()+"\n$";
                        SustemOut.clear();
                        ByteBuffer buff = ByteBuffer.wrap(str.getBytes());
                        channel.send(buff, from);
                        CU.setResponse("");
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

        public void setFrom(SocketAddress from) {
            this.from = from;
        }
    }

    /**
     * Класс, реализующий поток обработки запроса пользователя
     */
    static class ColThread extends Thread {
        private DatagramChannel channel;
        private SocketAddress from;
        private Map<String, SocketAddress> addressMap;
        private ByteBuffer buffer;
        public ColThread(String name,
                         DatagramChannel channel_, SocketAddress from, Map<String, SocketAddress> map, ByteBuffer byteBuffer) {
            super(name);
            this.channel=channel_;
            this.from=from;
            this.addressMap=map;
            this.buffer=byteBuffer;
        }

        public void run() {

            synchronized (sync) {
                ByteBuffer finalBuffer = ByteBuffer.allocate(buffer.position());
                if (getNameCom(buffer, finalBuffer, from).equals("login")) {
                    if (getAccess(buffer, finalBuffer, from).equals("DEFAULT")) {
                        try {
                            CheckLogin(bc.getCon(), channel, from, getLogin(buffer, finalBuffer, from), getPass(buffer, finalBuffer, from));
                            //System.out.println(getLogin(buffer, finalBuffer, from));
                        } catch (SQLException | IOException throwables) {
                            throwables.printStackTrace();
                        }
                    }else if (getAccess(buffer, finalBuffer, from).equals("TEST"))
                    {
                        SustemOut.addText("Nice cock");
                    }
                    else {
                        ConnectionKeies.replace(getAccess(buffer, finalBuffer, from).trim(), System.currentTimeMillis());
                        SustemOut.addText("Ты уже тута");
                        //str = SustemOut.sendTxt() + "\n$";
                        // printsmth(channel,from);
                    }
                } else if (getNameCom(buffer, finalBuffer, from).equals("sign_up")) {
                    try {
                        boolean b = true;
                        Statement stmt = bc.getCon().createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT * FROM JC_CONTACT");
                        while (rs.next()) {
                            if (rs.getString(2).equals(getLogin(buffer, finalBuffer, from))){
                                SustemOut.addText("Этот логин уже используется. Пожалуйста, используйте другой");
                                b = false;
                                break;
                            }
                        }if(b) {
                            insert(bc.getCon(), getLogin(buffer, finalBuffer, from), sCryptPasswordEncoder.encode(getPass(buffer, finalBuffer, from)));
                            ACCESS = UUID.randomUUID().toString();
                            ConnectionKeies.put(ACCESS, System.currentTimeMillis());
                            SustemOut.addText("Доступ открыт&"+ACCESS);
                        }
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }

                    //str = SustemOut.sendTxt() + "\n$";
                    /*try {
                        printsmth(channel, from);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }*/
                }else if(getNameCom(buffer, finalBuffer, from).equals("exit")) {
                    if(!(getAccess(buffer, finalBuffer, from).equals("DEFAULT")))
                        addressMap.remove(getLogin(buffer, finalBuffer, from));
                        sendToAll("Сервер покинул " + getLogin(buffer, finalBuffer, from), channel, getLogin(buffer, finalBuffer, from));
                }
                else if(getNameCom(buffer, finalBuffer, from).equals("help")) {
                    try {
                        action(buffer, finalBuffer, channel, from);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }else {

                        ConnectionKeies.forEach((k,v) -> {
                            if (k.equals(getAccess(buffer, finalBuffer, from).trim())) {

                                    try {
                                        action(buffer, finalBuffer, channel, from);
                                        ConnectionKeies.replace(k, System.currentTimeMillis());
                                    } catch (IOException | InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                AUTHORIZATIONCHECK = true;
                            }
                        });
                        if (!AUTHORIZATIONCHECK) {
                            SustemOut.addText("Авторизуйтесь для выполнения команд");
                            str = SustemOut.sendTxt() + "\n$";
                            //printsmth(channel,from);
                        }
                        AUTHORIZATIONCHECK = false;
                }
                transmitter.setFrom(from);

                    //sync2.notify();
                    //okonchaiya obrabotci zaprosa
                    //start otveta clentu
            }
        }
    }

    /**
     * Всопмогательный класс для многопоточной обработки запросов. Необходим для избежания ошибок во время обработки нескольких запросов одновременно
     */
    public static class ServerThread extends Thread{
        private DatagramChannel datagramChannel;
        private ThreadPoolExecutor sender;
        private Transmitter tr;
        private SocketAddress from;
        private ByteBuffer buffer;
        public ServerThread(DatagramChannel datagramChannel, ThreadPoolExecutor sender, Transmitter tr, SocketAddress from, ByteBuffer byteBuffer){
            this.datagramChannel = datagramChannel;
            this.sender=sender;
            this.tr=tr;
            this.from=from;
            this.buffer=byteBuffer;
            /*bufferThreadLocal.set(buffer);
            System.out.println(buffer.get(0));
            System.out.println(bufferThreadLocal.get().get(0));*/
        }
        public void run() {
            //System.out.println(buf.get(0));
            //System.out.println(bufferThreadLocal.get().get(0));
            ColThread colt = new ColThread("Исполнитель",  datagramChannel,from, addressMap, buffer);
            colt.start();
            /*executor.execute(rh);
            executor.execute(rh);*/
            try {
                colt.join();
            } catch (InterruptedException e) { e.printStackTrace(); }

            sender.execute(tr);
        }
    }

}