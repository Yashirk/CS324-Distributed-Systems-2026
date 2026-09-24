package cs324.client;

import cs324.common.JobRequest;
import cs324.common.JobResult;
import cs324.common.JobType;
import cs324.worker.WorkerService;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ClientGUI extends JFrame {

    // =====================================================
    // COLOURS
    // =====================================================

    private static final Color BACKGROUND =
            new Color(242, 245, 249);

    private static final Color NAVY =
            new Color(20, 35, 58);

    private static final Color NAVY_LIGHT =
            new Color(34, 52, 79);

    private static final Color ACCENT =
            new Color(0, 166, 153);

    private static final Color ACCENT_DARK =
            new Color(0, 130, 120);

    private static final Color WHITE =
            Color.WHITE;

    private static final Color TEXT =
            new Color(40, 48, 60);

    private static final Color MUTED =
            new Color(112, 122, 137);

    private static final Color SUCCESS =
            new Color(32, 145, 92);

    private static final Color ERROR =
            new Color(195, 55, 65);


    // =====================================================
    // GUI COMPONENTS
    // =====================================================

    private JComboBox<JobType> jobTypeBox;

    private JTextField numbersField;
    private JTextField startField;
    private JTextField endField;

    private JPanel inputCards;
    private CardLayout cardLayout;

    private JLabel resultLabel;
    private JLabel coordinatorLabel;
    private JLabel statusLabel;
    private JLabel jobTypeResultLabel;

    private JTextArea activityArea;

    private JButton loadCSVButton;
    private JButton submitButton;


    // =====================================================
    // THREAD POOL
    // =====================================================

    /*
     * Allows one GUI client to submit
     * multiple tasks concurrently.
     */
    private final ExecutorService clientPool =
            Executors.newCachedThreadPool();


    // =====================================================
    // CONSTRUCTOR
    // =====================================================

    public ClientGUI() {

        setTitle(
                "DistriLab | Distributed Computing Client"
        );

        setSize(
                920,
                650
        );

        setMinimumSize(
                new Dimension(
                        850,
                        600
                )
        );

        setLocationRelativeTo(null);

        setDefaultCloseOperation(
                JFrame.EXIT_ON_CLOSE
        );


        /*
         * Main background
         */
        JPanel root =
                new JPanel(
                        new BorderLayout()
                );

        root.setBackground(
                BACKGROUND
        );

        setContentPane(
                root
        );


        // HEADER
        root.add(
                createHeader(),
                BorderLayout.NORTH
        );


        // MAIN CONTENT
        JPanel content =
                new JPanel(
                        new BorderLayout(
                                18,
                                18
                        )
                );

        content.setBackground(
                BACKGROUND
        );

        content.setBorder(
                new EmptyBorder(
                        20,
                        25,
                        20,
                        25
                )
        );


        // TOP CARDS
        JPanel topCards =
                new JPanel(
                        new GridLayout(
                                1,
                                2,
                                18,
                                0
                        )
                );

        topCards.setOpaque(
                false
        );

        topCards.add(
                createJobCard()
        );

        topCards.add(
                createResultCard()
        );


        content.add(
                topCards,
                BorderLayout.NORTH
        );


        // ACTIVITY LOG
        content.add(
                createActivityCard(),
                BorderLayout.CENTER
        );


        root.add(
                content,
                BorderLayout.CENTER
        );


        // BOTTOM STATUS BAR
        root.add(
                createStatusBar(),
                BorderLayout.SOUTH
        );


        updateInputPanel();


        addWindowListener(
                new WindowAdapter() {

                    @Override
                    public void windowClosing(
                            WindowEvent e) {

                        clientPool.shutdownNow();
                    }
                }
        );
    }


    // =====================================================
    // HEADER
    // =====================================================

    private JPanel createHeader() {

        JPanel header =
                new GradientPanel();

        header.setLayout(
                new BorderLayout()
        );

        header.setBorder(
                new EmptyBorder(
                        18,
                        28,
                        18,
                        28
                )
        );


        // LEFT SIDE
        JPanel titles =
                new JPanel();

        titles.setOpaque(
                false
        );

        titles.setLayout(
                new BoxLayout(
                        titles,
                        BoxLayout.Y_AXIS
                )
        );


        JLabel brand =
                new JLabel(
                        "DistriLab"
                );

        brand.setForeground(
                WHITE
        );

        brand.setFont(
                new Font(
                        "Segoe UI",
                        Font.BOLD,
                        27
                )
        );


        JLabel subtitle =
                new JLabel(
                        "Distributed Computing Client"
                );

        subtitle.setForeground(
                new Color(
                        195,
                        210,
                        225
                )
        );

        subtitle.setFont(
                new Font(
                        "Segoe UI",
                        Font.PLAIN,
                        14
                )
        );


        titles.add(
                brand
        );

        titles.add(
                Box.createVerticalStrut(
                        3
                )
        );

        titles.add(
                subtitle
        );


        header.add(
                titles,
                BorderLayout.WEST
        );


        // RIGHT SIDE
        JLabel systemBadge =
                new JLabel(
                        "  JAVA RMI CLIENT  "
                );

        systemBadge.setOpaque(
                true
        );

        systemBadge.setBackground(
                ACCENT
        );

        systemBadge.setForeground(
                WHITE
        );

        systemBadge.setFont(
                new Font(
                        "Segoe UI",
                        Font.BOLD,
                        12
                )
        );

        systemBadge.setBorder(
                new EmptyBorder(
                        8,
                        10,
                        8,
                        10
                )
        );


        header.add(
                systemBadge,
                BorderLayout.EAST
        );


        return header;
    }


    // =====================================================
    // JOB CONFIGURATION CARD
    // =====================================================

    private JPanel createJobCard() {

        JPanel card =
                createCard();


        card.setLayout(
                new BoxLayout(
                        card,
                        BoxLayout.Y_AXIS
                )
        );


        JLabel heading =
                sectionTitle(
                        "Job Configuration"
                );


        JLabel help =
                sectionSubtitle(
                        "Select a computation and provide input data."
                );


        card.add(
                heading
        );

        card.add(
                Box.createVerticalStrut(
                        3
                )
        );

        card.add(
                help
        );

        card.add(
                Box.createVerticalStrut(
                        18
                )
        );


        // JOB TYPE LABEL
        JLabel typeLabel =
                fieldLabel(
                        "JOB TYPE"
                );

        card.add(
                typeLabel
        );

        card.add(
                Box.createVerticalStrut(
                        6
                )
        );


        jobTypeBox =
                new JComboBox<>(
                        JobType.values()
                );

        styleComboBox(
                jobTypeBox
        );

        jobTypeBox.setMaximumSize(
                new Dimension(
                        Integer.MAX_VALUE,
                        38
                )
        );

        card.add(
                jobTypeBox
        );

        card.add(
                Box.createVerticalStrut(
                        15
                )
        );


        // INPUT CARDS
        cardLayout =
                new CardLayout();

        inputCards =
                new JPanel(
                        cardLayout
                );

        inputCards.setOpaque(
                false
        );

        inputCards.setMaximumSize(
                new Dimension(
                        Integer.MAX_VALUE,
                        95
                )
        );


        inputCards.add(
                createListInputPanel(),
                "LIST"
        );

        inputCards.add(
                createRangeInputPanel(),
                "RANGE"
        );


        card.add(
                inputCards
        );

        card.add(
                Box.createVerticalStrut(
                        16
                )
        );


        // BUTTONS
        JPanel buttons =
                new JPanel(
                        new GridLayout(
                                1,
                                2,
                                10,
                                0
                        )
                );

        buttons.setOpaque(
                false
        );


        loadCSVButton =
                createSecondaryButton(
                        "Load CSV"
                );


        submitButton =
                createPrimaryButton(
                        "Submit Job"
                );


        buttons.add(
                loadCSVButton
        );

        buttons.add(
                submitButton
        );


        card.add(
                buttons
        );


        // EVENTS
        jobTypeBox.addActionListener(
                e ->
                        updateInputPanel()
        );


        loadCSVButton.addActionListener(
                e ->
                        loadCSV()
        );


        submitButton.addActionListener(
                e ->
                        submitJob()
        );


        return card;
    }


    // =====================================================
    // LIST INPUT
    // =====================================================

    private JPanel createListInputPanel() {

        JPanel panel =
                new JPanel();

        panel.setOpaque(
                false
        );

        panel.setLayout(
                new BoxLayout(
                        panel,
                        BoxLayout.Y_AXIS
                )
        );


        JLabel label =
                fieldLabel(
                        "NUMBERS"
                );


        numbersField =
                createTextField();

        numbersField.setToolTipText(
                "Example: 12, 45, 7, 99, 23"
        );


        panel.add(
                label
        );

        panel.add(
                Box.createVerticalStrut(
                        6
                )
        );

        panel.add(
                numbersField
        );


        return panel;
    }


    // =====================================================
    // RANGE INPUT
    // =====================================================

    private JPanel createRangeInputPanel() {

        JPanel panel =
                new JPanel();

        panel.setOpaque(
                false
        );

        panel.setLayout(
                new BoxLayout(
                        panel,
                        BoxLayout.Y_AXIS
                )
        );


        JLabel label =
                fieldLabel(
                        "PRIME RANGE"
                );


        panel.add(
                label
        );

        panel.add(
                Box.createVerticalStrut(
                        6
                )
        );


        JPanel fields =
                new JPanel(
                        new GridLayout(
                                1,
                                2,
                                10,
                                0
                        )
                );

        fields.setOpaque(
                false
        );


        startField =
                createTextField();

        startField.setToolTipText(
                "Start of range"
        );


        endField =
                createTextField();

        endField.setToolTipText(
                "End of range"
        );


        fields.add(
                startField
        );

        fields.add(
                endField
        );


        panel.add(
                fields
        );


        return panel;
    }


    // =====================================================
    // RESULT CARD
    // =====================================================

    private JPanel createResultCard() {

        JPanel card =
                createCard();


        card.setLayout(
                new BoxLayout(
                        card,
                        BoxLayout.Y_AXIS
                )
        );


        JLabel title =
                sectionTitle(
                        "Latest Result"
                );


        JLabel subtitle =
                sectionSubtitle(
                        "Output from the distributed worker network."
                );


        card.add(
                title
        );

        card.add(
                Box.createVerticalStrut(
                        3
                )
        );

        card.add(
                subtitle
        );

        card.add(
                Box.createVerticalStrut(
                        25
                )
        );


        jobTypeResultLabel =
                new JLabel(
                        "NO JOB SUBMITTED"
                );

        jobTypeResultLabel.setForeground(
                MUTED
        );

        jobTypeResultLabel.setFont(
                new Font(
                        "Segoe UI",
                        Font.BOLD,
                        12
                )
        );

        jobTypeResultLabel.setAlignmentX(
                Component.CENTER_ALIGNMENT
        );


        resultLabel =
                new JLabel(
                        "—"
                );

        resultLabel.setForeground(
                NAVY
        );

        resultLabel.setFont(
                new Font(
                        "Segoe UI",
                        Font.BOLD,
                        46
                )
        );

        resultLabel.setAlignmentX(
                Component.CENTER_ALIGNMENT
        );


        coordinatorLabel =
                new JLabel(
                        "Coordinator: —"
                );

        coordinatorLabel.setForeground(
                MUTED
        );

        coordinatorLabel.setFont(
                new Font(
                        "Segoe UI",
                        Font.PLAIN,
                        14
                )
        );

        coordinatorLabel.setAlignmentX(
                Component.CENTER_ALIGNMENT
        );


        card.add(
                jobTypeResultLabel
        );

        card.add(
                Box.createVerticalStrut(
                        8
                )
        );

        card.add(
                resultLabel
        );

        card.add(
                Box.createVerticalStrut(
                        12
                )
        );

        card.add(
                coordinatorLabel
        );


        return card;
    }


    // =====================================================
    // ACTIVITY CARD
    // =====================================================

    private JPanel createActivityCard() {

        JPanel card =
                createCard();


        card.setLayout(
                new BorderLayout(
                        0,
                        10
                )
        );


        JPanel headingPanel =
                new JPanel(
                        new BorderLayout()
                );

        headingPanel.setOpaque(
                false
        );


        JLabel heading =
                sectionTitle(
                        "Task Activity"
                );


        JLabel description =
                sectionSubtitle(
                        "Live history of jobs submitted by this client."
                );


        JPanel titleGroup =
                new JPanel();

        titleGroup.setOpaque(
                false
        );

        titleGroup.setLayout(
                new BoxLayout(
                        titleGroup,
                        BoxLayout.Y_AXIS
                )
        );


        titleGroup.add(
                heading
        );

        titleGroup.add(
                description
        );


        headingPanel.add(
                titleGroup,
                BorderLayout.WEST
        );


        JButton clearButton =
                new JButton(
                        "Clear"
                );

        clearButton.setFocusPainted(
                false
        );

        clearButton.addActionListener(
                e ->
                        activityArea.setText("")
        );


        headingPanel.add(
                clearButton,
                BorderLayout.EAST
        );


        card.add(
                headingPanel,
                BorderLayout.NORTH
        );


        activityArea =
                new JTextArea();

        activityArea.setEditable(
                false
        );

        activityArea.setFont(
                new Font(
                        "Consolas",
                        Font.PLAIN,
                        13
                )
        );

        activityArea.setForeground(
                TEXT
        );

        activityArea.setBackground(
                new Color(
                        248,
                        250,
                        252
                )
        );

        activityArea.setBorder(
                new EmptyBorder(
                        12,
                        12,
                        12,
                        12
                )
        );


        JScrollPane scroll =
                new JScrollPane(
                        activityArea
                );

        scroll.setBorder(
                BorderFactory.createLineBorder(
                        new Color(
                                220,
                                225,
                                232
                        )
                )
        );


        card.add(
                scroll,
                BorderLayout.CENTER
        );


        return card;
    }


    // =====================================================
    // STATUS BAR
    // =====================================================

    private JPanel createStatusBar() {

        JPanel bar =
                new JPanel(
                        new BorderLayout()
                );

        bar.setBackground(
                NAVY
        );

        bar.setBorder(
                new EmptyBorder(
                        9,
                        25,
                        9,
                        25
                )
        );


        statusLabel =
                new JLabel(
                        "● READY"
                );

        statusLabel.setForeground(
                new Color(
                        111,
                        224,
                        178
                )
        );

        statusLabel.setFont(
                new Font(
                        "Segoe UI",
                        Font.BOLD,
                        12
                )
        );


        JLabel networkLabel =
                new JLabel(
                        "DistriLab Network  |  Java RMI"
                );

        networkLabel.setForeground(
                new Color(
                        180,
                        194,
                        210
                )
        );

        networkLabel.setFont(
                new Font(
                        "Segoe UI",
                        Font.PLAIN,
                        12
                )
        );


        bar.add(
                statusLabel,
                BorderLayout.WEST
        );

        bar.add(
                networkLabel,
                BorderLayout.EAST
        );


        return bar;
    }


    // =====================================================
    // INPUT TYPE SWITCHING
    // =====================================================

    private void updateInputPanel() {

        JobType type =
                (JobType)
                        jobTypeBox
                                .getSelectedItem();


        if (type
                == JobType.PRIMESUM) {

            cardLayout.show(
                    inputCards,
                    "RANGE"
            );

            loadCSVButton.setText(
                    "Load Range CSV"
            );

        } else {

            cardLayout.show(
                    inputCards,
                    "LIST"
            );

            loadCSVButton.setText(
                    "Load CSV"
            );
        }
    }


    // =====================================================
    // CSV LOADING
    // =====================================================

    private void loadCSV() {

        JFileChooser chooser =
                new JFileChooser();


        int choice =
                chooser.showOpenDialog(
                        this
                );


        if (choice
                != JFileChooser.APPROVE_OPTION) {

            return;
        }


        try {

            File file =
                    chooser.getSelectedFile();


            List<Integer> numbers =
                    CSVLoader.loadNumbers(
                            file
                    );


            JobType type =
                    (JobType)
                            jobTypeBox
                                    .getSelectedItem();


            if (type
                    == JobType.PRIMESUM) {

                if (numbers.size() < 2) {

                    throw new Exception(
                            "PRIMESUM CSV requires a start and end value."
                    );
                }


                startField.setText(
                        String.valueOf(
                                numbers.get(0)
                        )
                );


                endField.setText(
                        String.valueOf(
                                numbers.get(1)
                        )
                );


            } else {

                String text =
                        numbers.stream()
                                .map(
                                        String::valueOf
                                )
                                .collect(
                                        Collectors.joining(
                                                ", "
                                        )
                                );


                numbersField.setText(
                        text
                );
            }


            updateStatus(
                    "CSV LOADED",
                    ACCENT
            );


            appendActivity(
                    "[CSV] Loaded: "
                            + file.getName()
            );


        } catch (Exception e) {

            showError(
                    e.getMessage()
            );
        }
    }


    // =====================================================
    // BUILD JOB
    // =====================================================

    private JobRequest buildJob()
            throws Exception {

        JobType type =
                (JobType)
                        jobTypeBox
                                .getSelectedItem();


        if (type
                == JobType.PRIMESUM) {

            String startText =
                    startField
                            .getText()
                            .trim();


            String endText =
                    endField
                            .getText()
                            .trim();


            if (startText.isEmpty()
                    ||
                    endText.isEmpty()) {

                throw new Exception(
                        "Please enter both start and end values."
                );
            }


            int start =
                    Integer.parseInt(
                            startText
                    );


            int end =
                    Integer.parseInt(
                            endText
                    );


            if (start > end) {

                throw new Exception(
                        "Start value cannot be greater than end value."
                );
            }


            return new JobRequest(
                    JobType.PRIMESUM,
                    start,
                    end
            );
        }


        String input =
                numbersField
                        .getText()
                        .trim();


        if (input.isEmpty()) {

            throw new Exception(
                    "Please enter at least one number."
            );
        }


        String[] pieces =
                input.split(
                        "[,\\s]+"
                );


        List<Integer> numbers =
                new ArrayList<>();


        for (String piece : pieces) {

            if (!piece.isBlank()) {

                numbers.add(
                        Integer.parseInt(
                                piece
                        )
                );
            }
        }


        if (numbers.isEmpty()) {

            throw new Exception(
                    "No valid numbers were entered."
            );
        }


        return new JobRequest(
                type,
                numbers
        );
    }


    // =====================================================
    // SUBMIT JOB
    // =====================================================

    private void submitJob() {

        final JobRequest job;


        try {

            job =
                    buildJob();


        } catch (Exception e) {

            showError(
                    "Invalid input.\n"
                            + e.getMessage()
            );

            return;
        }


        appendActivity(
                "[SUBMITTED] "
                        + job.getJobType()
                        + " | "
                        + shortJobId(
                        job.getJobId()
                )
        );


        updateStatus(
                "PROCESSING",
                new Color(
                        235,
                        175,
                        55
                )
        );


        submitButton.setText(
                "Processing..."
        );


        /*
         * Job runs outside Swing's UI thread.
         *
         * Multiple clicks can therefore create
         * multiple concurrent client tasks.
         */
        clientPool.submit(
                () -> {

                    try {

                        WorkerService coordinator =
                                ClientConnection
                                        .findCoordinator();


                        JobResult result =
                                coordinator.submitJob(
                                        job
                                );


                        SwingUtilities.invokeLater(
                                () -> {

                                    jobTypeResultLabel.setText(
                                            job.getJobType()
                                                    .toString()
                                                    + " RESULT"
                                    );


                                    resultLabel.setText(
                                            String.valueOf(
                                                    result.getResult()
                                            )
                                    );


                                    coordinatorLabel.setText(
                                            "Coordinator: Worker "
                                                    + result.getCoordinatorId()
                                    );


                                    updateStatus(
                                            "COMPLETED",
                                            SUCCESS
                                    );


                                    submitButton.setText(
                                            "Submit Job"
                                    );
                                }
                        );


                        appendActivity(
                                "[COMPLETED] "
                                        + job.getJobType()
                                        + " → "
                                        + result.getResult()
                                        + " | Coordinator W"
                                        + result.getCoordinatorId()
                        );


                    } catch (Exception e) {

                        SwingUtilities.invokeLater(
                                () -> {

                                    resultLabel.setText(
                                            "ERROR"
                                    );

                                    resultLabel.setForeground(
                                            ERROR
                                    );


                                    updateStatus(
                                            "FAILED",
                                            ERROR
                                    );


                                    submitButton.setText(
                                            "Submit Job"
                                    );
                                }
                        );


                        appendActivity(
                                "[FAILED] "
                                        + job.getJobType()
                                        + " → "
                                        + e.getMessage()
                        );
                    }
                }
        );
    }


    // =====================================================
    // ACTIVITY
    // =====================================================

    private void appendActivity(
            String text) {

        SwingUtilities.invokeLater(
                () -> {

                    activityArea.append(
                            text + "\n"
                    );


                    activityArea.setCaretPosition(
                            activityArea
                                    .getDocument()
                                    .getLength()
                    );
                }
        );
    }


    // =====================================================
    // STATUS
    // =====================================================

    private void updateStatus(
            String text,
            Color colour) {

        SwingUtilities.invokeLater(
                () -> {

                    statusLabel.setText(
                            "● " + text
                    );

                    statusLabel.setForeground(
                            colour
                    );
                }
        );
    }


    // =====================================================
    // ERROR
    // =====================================================

    private void showError(
            String message) {

        JOptionPane.showMessageDialog(
                this,
                message,
                "DistriLab Error",
                JOptionPane.ERROR_MESSAGE
        );
    }


    // =====================================================
    // HELPERS
    // =====================================================

    private String shortJobId(
            String jobId) {

        if (jobId == null
                ||
                jobId.length() <= 8) {

            return jobId;
        }

        return jobId.substring(
                0,
                8
        );
    }


    private JPanel createCard() {

        JPanel panel =
                new JPanel();

        panel.setBackground(
                WHITE
        );

        panel.setBorder(
                BorderFactory.createCompoundBorder(

                        BorderFactory.createLineBorder(
                                new Color(
                                        224,
                                        229,
                                        236
                                )
                        ),

                        new EmptyBorder(
                                20,
                                20,
                                20,
                                20
                        )
                )
        );

        return panel;
    }


    private JLabel sectionTitle(
            String text) {

        JLabel label =
                new JLabel(
                        text
                );

        label.setForeground(
                NAVY
        );

        label.setFont(
                new Font(
                        "Segoe UI",
                        Font.BOLD,
                        18
                )
        );

        return label;
    }


    private JLabel sectionSubtitle(
            String text) {

        JLabel label =
                new JLabel(
                        text
                );

        label.setForeground(
                MUTED
        );

        label.setFont(
                new Font(
                        "Segoe UI",
                        Font.PLAIN,
                        12
                )
        );

        return label;
    }


    private JLabel fieldLabel(
            String text) {

        JLabel label =
                new JLabel(
                        text
                );

        label.setForeground(
                MUTED
        );

        label.setFont(
                new Font(
                        "Segoe UI",
                        Font.BOLD,
                        11
                )
        );

        return label;
    }


    private JTextField createTextField() {

        JTextField field =
                new JTextField();

        field.setFont(
                new Font(
                        "Segoe UI",
                        Font.PLAIN,
                        14
                )
        );

        field.setForeground(
                TEXT
        );

        field.setBackground(
                new Color(
                        248,
                        250,
                        252
                )
        );

        field.setBorder(
                BorderFactory.createCompoundBorder(

                        BorderFactory.createLineBorder(
                                new Color(
                                        210,
                                        217,
                                        226
                                )
                        ),

                        new EmptyBorder(
                                9,
                                10,
                                9,
                                10
                        )
                )
        );

        field.setMaximumSize(
                new Dimension(
                        Integer.MAX_VALUE,
                        40
                )
        );

        return field;
    }


    private void styleComboBox(
            JComboBox<?> comboBox) {

        comboBox.setFont(
                new Font(
                        "Segoe UI",
                        Font.PLAIN,
                        14
                )
        );

        comboBox.setBackground(
                WHITE
        );

        comboBox.setForeground(
                TEXT
        );
    }


    private JButton createPrimaryButton(
            String text) {

        JButton button =
                new JButton(
                        text
                );

        button.setFont(
                new Font(
                        "Segoe UI",
                        Font.BOLD,
                        13
                )
        );

        button.setForeground(
                WHITE
        );

        button.setBackground(
                ACCENT
        );

        button.setFocusPainted(
                false
        );

        button.setBorder(
                new EmptyBorder(
                        11,
                        15,
                        11,
                        15
                )
        );

        button.setCursor(
                Cursor.getPredefinedCursor(
                        Cursor.HAND_CURSOR
                )
        );

        return button;
    }


    private JButton createSecondaryButton(
            String text) {

        JButton button =
                new JButton(
                        text
                );

        button.setFont(
                new Font(
                        "Segoe UI",
                        Font.BOLD,
                        13
                )
        );

        button.setForeground(
                NAVY
        );

        button.setBackground(
                new Color(
                        232,
                        237,
                        243
                )
        );

        button.setFocusPainted(
                false
        );

        button.setBorder(
                new EmptyBorder(
                        11,
                        15,
                        11,
                        15
                )
        );

        button.setCursor(
                Cursor.getPredefinedCursor(
                        Cursor.HAND_CURSOR
                )
        );

        return button;
    }


    // =====================================================
    // GRADIENT HEADER
    // =====================================================

    private static class GradientPanel
            extends JPanel {

        @Override
        protected void paintComponent(
                Graphics g) {

            Graphics2D g2 =
                    (Graphics2D)
                            g.create();


            GradientPaint gradient =
                    new GradientPaint(
                            0,
                            0,
                            NAVY,
                            getWidth(),
                            0,
                            NAVY_LIGHT
                    );


            g2.setPaint(
                    gradient
            );


            g2.fillRect(
                    0,
                    0,
                    getWidth(),
                    getHeight()
            );


            g2.dispose();


            super.paintComponent(
                    g
            );
        }


        @Override
        public boolean isOpaque() {

            return false;
        }
    }


    // =====================================================
    // MAIN
    // =====================================================

    public static void main(
            String[] args) {


        try {

            UIManager.setLookAndFeel(
                    UIManager
                            .getSystemLookAndFeelClassName()
            );

        } catch (Exception ignored) {

        }


        SwingUtilities.invokeLater(
                () -> {

                    ClientGUI gui =
                            new ClientGUI();


                    gui.setVisible(
                            true
                    );
                }
        );
    }
}