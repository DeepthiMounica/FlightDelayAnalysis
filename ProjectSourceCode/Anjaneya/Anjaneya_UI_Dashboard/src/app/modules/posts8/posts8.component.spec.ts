import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Posts8Component } from './posts8.component';

describe('Posts8Component', () => {
  let component: Posts8Component;
  let fixture: ComponentFixture<Posts8Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Posts8Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Posts8Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
