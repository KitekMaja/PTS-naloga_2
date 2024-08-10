package um.si.reservation.info;

public class ReservationInfo {
    private final String reservationId;
    private final String screeningId;
    private final int numberOfTickets;
    private final String name;
    private final String surname;
    private final String email;
    public ReservationInfo(String reservationId, String screeningId, int numberOfTickets, String name, String surname, String email) {
        this.reservationId = reservationId;
        this.screeningId = screeningId;
        this.numberOfTickets = numberOfTickets;
        this.name = name;
        this.surname = surname;
        this.email = email;
    }

    public String getReservationId() {
        return reservationId;
    }

    public int getNumberOfTickets() {
        return numberOfTickets;
    }

    public String getName() {
        return name;
    }

    public String getSurname() {
        return surname;
    }

    public String getEmail() {
        return email;
    }

    public String getScreeningId() {
        return screeningId;
    }

    @Override
    public String toString() {
        return "ReservationInfo{" +
                "reservationId='" + reservationId + '\'' +
                ", screeningId='" + screeningId + '\'' +
                ", numberOfTickets=" + numberOfTickets +
                ", name='" + name + '\'' +
                ", surname='" + surname + '\'' +
                ", email='" + email + '\'' +
                '}';
    }
}