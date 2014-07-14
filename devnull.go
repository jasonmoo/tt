package main

type (
	WriteFlusher interface {
		Write([]byte) (int, error)
		WriteByte(byte) error
		WriteString(string) (int, error)
		Flush() error
	}
	DevNullWriter bool
)

func (d *DevNullWriter) Write(_ []byte) (int, error)       { return 0, nil }
func (d *DevNullWriter) WriteByte(_ byte) error            { return nil }
func (d *DevNullWriter) WriteString(_ string) (int, error) { return 0, nil }
func (d *DevNullWriter) Flush() error                      { return nil }
