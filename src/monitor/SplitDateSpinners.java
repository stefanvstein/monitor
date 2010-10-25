package monitor;
import java.awt.Component;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.util.Calendar;
import java.util.Date;

import javax.swing.JComponent;
import javax.swing.JFormattedTextField;
import javax.swing.JSpinner;
import javax.swing.JSpinner.NumberEditor;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingUtilities;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.text.JTextComponent;

public class SplitDateSpinners {
	private Calendar temp = Calendar.getInstance();

	
	private Calendar calendar;
	private boolean enabled = true;
	private SpinnerNumberModel yearModel = new SpinnerNumberModel();
	private SpinnerNumberModel monthModel = new SpinnerNumberModel();
	private SpinnerNumberModel dayModel = new SpinnerNumberModel();
	private SpinnerNumberModel secondModel = new SpinnerNumberModel();
	private SpinnerNumberModel minuteModel = new SpinnerNumberModel();
	private SpinnerNumberModel hourModel = new SpinnerNumberModel();
	private Component yearSpinner;
	private Component monthSpinner;
	private Component daySpinner;
	private Component hourSpinner;
	private Component minuteSpinner;
	private Component secondSpinner;
	private final Date upper;
	private final Date lower;
	private final boolean walkToEdge;
	public SplitDateSpinners(Date date, Date upper, Date lower){
		this(date, upper, lower, true);
	}
	
	public SplitDateSpinners(Date date, Date upper, Date lower, boolean walkToEdge) {
		// assertVerify
		if (date.getTime() < lower.getTime() || date.getTime() > upper.getTime())
			throw new IllegalArgumentException("date has to be between upper and lower");
		calendar = Calendar.getInstance();
		calendar.setTime(date);
		this.upper = upper;
		this.lower = lower;
		this.walkToEdge=walkToEdge;
		updateAll();
		yearModel.addChangeListener(changeListener(Calendar.YEAR, yearModel));
		monthModel
				.addChangeListener(changeListener(Calendar.MONTH, monthModel));
		dayModel.addChangeListener(changeListener(Calendar.DAY_OF_MONTH,
				dayModel));
		hourModel.addChangeListener(changeListener(Calendar.HOUR_OF_DAY,
				hourModel));
		minuteModel.addChangeListener(changeListener(Calendar.MINUTE,
				minuteModel));
		secondModel.addChangeListener(changeListener(Calendar.SECOND,
				secondModel));

		yearSpinner = createSpinner(yearModel, "0000");
		monthSpinner = createSpinner(monthModel, "00");
		daySpinner = createSpinner(dayModel, "00");
		hourSpinner = createSpinner(hourModel, "00");
		minuteSpinner = createSpinner(minuteModel, "00");
		secondSpinner = createSpinner(secondModel, "00");
	}

	private ChangeListener changeListener(final int calendarField,
			final SpinnerNumberModel model) {
		return new ChangeListener() {
			public void stateChanged(ChangeEvent e) {
				if (enabled) {
					temp.setTime(calendar.getTime());
					int value = model.getNumber().intValue();
					if (calendarField == Calendar.MONTH)
						value--;
					int distance = calendar.get(calendarField)-value;
					temp.roll(calendarField, distance);
					
					;temp.set(calendarField, value);
					Date newDate = verify(upper, lower, temp.getTime());
					if (null != newDate)
						calendar.setTime(newDate);
					enabled = false;
					updateAll();
					enabled = true;
				}
			}
		};
	}

	private Date verify(final Date upper, final Date lower, final Date subject) {
		if (subject.getTime() < lower.getTime()) {
			if (walkToEdge)
				return lower;
			return null;
		}
		if (subject.getTime() > upper.getTime()) {
			if (walkToEdge)
				return upper;
			return null;
		}
		return subject;
	}

	public Component yearSpinner() {
		return yearSpinner;
	}

	public Component monthSpinner() {
		return monthSpinner;
	}

	public Component daySpinner() {
		return daySpinner;
	}

	public Component hourSpinner() {
		return hourSpinner;
	}

	public Component minuteSpinner() {
		return minuteSpinner;
	}

	public Component secondSpinner() {
		return secondSpinner;
	}

	private void updateAll() {

		secondModel.setValue(calendar.get(Calendar.SECOND));
		minuteModel.setValue(calendar.get(Calendar.MINUTE));
		hourModel.setValue(calendar.get(Calendar.HOUR_OF_DAY));
		dayModel.setValue(calendar.get(Calendar.DAY_OF_MONTH));
		monthModel.setValue(1 + calendar.get(Calendar.MONTH));
		yearModel.setValue(calendar.get(Calendar.YEAR));
	}

    public void enable(boolean enabled){
	secondSpinner.setEnabled(enabled);
	minuteSpinner.setEnabled(enabled);
	hourSpinner.setEnabled(enabled);
	daySpinner.setEnabled(enabled);
	monthSpinner.setEnabled(enabled);
	yearSpinner.setEnabled(enabled);
    }

	public Date getDate() {
		return calendar.getTime();
	}

	private JComponent createSpinner(SpinnerNumberModel model, String string) {
		JSpinner spinner = new JSpinner(model);
		NumberEditor editor = new JSpinner.NumberEditor(spinner, string);
		spinner.setEditor(editor);

		JFormattedTextField a = editor.getTextField();
		a.addFocusListener(new FocusAdapter() {

			@Override
			public void focusLost(FocusEvent e) {
				super.focusLost(e);
			}

			@Override
			public void focusGained(FocusEvent e) {
				super.focusGained(e);
				if (e.getSource() instanceof JTextComponent) {
					final JTextComponent textComponent = ((JTextComponent) e
							.getSource());
					SwingUtilities.invokeLater(new Runnable() {
						public void run() {
							textComponent.selectAll();
						}
					});
				}
			}
		});
		return spinner;
	}
}
